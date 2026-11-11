use std::{
  collections::{BTreeMap, HashMap, HashSet},
  time::{Duration, Instant},
};

use tokio::{
  select,
  sync::mpsc::{UnboundedReceiver, error::TrySendError},
  task::JoinHandle,
  time::interval,
};

use crate::{
  event::event::TorrentEvent,
  peers::{
    client::BLOCK_SIZE,
    peer::{
      PeerCommand::{self},
      PeerHandle,
    },
  },
  pieces::{
    bitfield::{add_piece, has_piece},
    piece_geometry::PieceGeometry,
  },
  storage_manager::BlockMeta,
};

const MIN_DEPTH: usize = 10;
const MAX_DEPTH: usize = 1000;
const TARGET_QUEUE_SECS: f64 = 4.0; // keep ~4s of data queued per peer
const REQUEST_TIMEOUT: Duration = Duration::from_secs(7);
const MEMORY_BUDGET: u64 = 125 * 1024 * 1024;

#[derive(Default)]
struct RateInfo {
  blocks: u32,
  per_sec: f64,
}

pub struct Scheduler {
  max_active_pieces: u16,
  pieces_in_slot: HashSet<u32>, // pieces we are currently downloading
  pending: BTreeMap<u32, Vec<u32>>, // piece -> offsets still to hand out (lowest piece first)

  active_peers: HashSet<PeerHandle>, // shared with PeerManager
  unchoked: HashSet<PeerHandle>,
  verified_pieces: Vec<u8>,
  first_unverified: u32,

  requested: HashMap<(u32, u32), (PeerHandle, Instant)>, // (piece, offset) -> who, when
  received: HashSet<(u32, u32)>,
  per_peer: HashMap<PeerHandle, usize>, // in-flight count per peer
  peer_rate: HashMap<PeerHandle, RateInfo>,
  last_sweep: Instant,

  cooldown: HashMap<u32, Instant>,
  fail_count: HashMap<u32, u32>,

  event_queue: UnboundedReceiver<TorrentEvent>,
  total_pieces: u32,
  geometry: PieceGeometry,
}

impl Scheduler {
  pub fn new(
    event_queue: UnboundedReceiver<TorrentEvent>,
    total_length: u64,
    piece_length: u32,
  ) -> Self {
    let by_memory = (MEMORY_BUDGET / piece_length.max(1) as u64).clamp(4, u16::MAX as u64) as u16;
    let num_pieces = total_length.div_ceil(piece_length as u64);

    Self {
      max_active_pieces: by_memory,
      pieces_in_slot: HashSet::new(),
      pending: BTreeMap::new(),
      active_peers: HashSet::new(),
      unchoked: HashSet::new(),
      verified_pieces: vec![0_u8; ((num_pieces + 7) >> 3) as usize],
      first_unverified: 0,
      requested: HashMap::new(),
      received: HashSet::new(),
      per_peer: HashMap::new(),
      peer_rate: HashMap::new(),
      last_sweep: Instant::now(),
      cooldown: HashMap::new(),
      fail_count: HashMap::new(),
      event_queue,
      total_pieces: total_length.div_ceil(piece_length as u64) as u32,
      geometry: PieceGeometry::new(total_length, piece_length),
    }
  }

  pub fn run(mut self) -> JoinHandle<()> {
    tokio::spawn(async move {
      let mut sweep = interval(Duration::from_secs(1));
      loop {
        select! {
          event = self.event_queue.recv() => match event {
            Some(TorrentEvent::DownloadComplete) | None => break,
            Some(event) => self.reducer(event), // refills only the peer it concerns
          },
          _ = sweep.tick() => {
            self.update_rates();
            self.release_stale();
            self.schedule();
          },
        }
      }
    })
  }

  //mutates state
  fn reducer(&mut self, event: TorrentEvent) {
    match event {
      TorrentEvent::Unchoke(peer) => {
        self.unchoked.insert(peer.clone());
        self.schedule();
      }
      TorrentEvent::Choke(peer) => {
        self.unchoked.remove(&peer);
        self.release_requests_of(&peer);
      }
      TorrentEvent::Disconnect(peer) => {
        self.active_peers.remove(&peer);
        self.unchoked.remove(&peer);
        self.peer_rate.remove(&peer);
        self.release_requests_of(&peer);
      }
      TorrentEvent::BlockReceive(piece) => {
        if !self.pieces_in_slot.contains(&piece.piece_idx) {
          return; // late block for a piece we're no longer working on
        }
        let key = (piece.piece_idx, piece.offset);
        self.received.insert(key);

        // a re-queued duplicate may still be waiting in pending
        if let Some(offs) = self.pending.get_mut(&piece.piece_idx) {
          offs.retain(|o| *o != piece.offset);
        }

        if let Some((peer, _)) = self.requested.remove(&key) {
          self.dec_inflight(&peer);
          self.peer_rate.entry(peer.clone()).or_default().blocks += 1;
          self.request_peer(&peer); // top this peer up right away ⚡
        }
      }
      TorrentEvent::PieceComplete(piece_idx, _len) => {
        add_piece(&mut self.verified_pieces, piece_idx as usize);
        self.forget_piece(piece_idx);
        self.fail_count.remove(&piece_idx);
        self.cooldown.remove(&piece_idx);
      }
      TorrentEvent::PieceFail(piece_idx) => {
        // free the slot and back off before retrying 🔁
        self.forget_piece(piece_idx);
        let n = self.fail_count.entry(piece_idx).or_insert(0);
        *n += 1;
        let secs = 2u64.pow((*n).min(6)).min(60);
        self
          .cooldown
          .insert(piece_idx, Instant::now() + Duration::from_secs(secs));
      }
      TorrentEvent::Exists(idx, _len) => {
        add_piece(&mut self.verified_pieces, idx as usize);
        self.forget_piece(idx);
      }
      TorrentEvent::Bitfield => {
        self.schedule();
      }
      TorrentEvent::Connect(peer) => {
        self.active_peers.insert(peer);
      }
      _ => {}
    }
  }

  fn dec_inflight(&mut self, peer: &PeerHandle) {
    if let Some(n) = self.per_peer.get_mut(peer) {
      *n = n.saturating_sub(1);
    }
  }

  /// piece finished or failed: forget every trace of it
  fn forget_piece(&mut self, piece_idx: u32) {
    self.received.retain(|(p, _)| *p != piece_idx);

    let keys: Vec<(u32, u32)> = self
      .requested
      .keys()
      .filter(|(p, _)| *p == piece_idx)
      .copied()
      .collect();
    for key in keys {
      if let Some((peer, _)) = self.requested.remove(&key) {
        self.dec_inflight(&peer);
      }
    }

    self.pending.remove(&piece_idx);
    self.pieces_in_slot.remove(&piece_idx);
  }

  /// put a block back in the pool, but only if it's still wanted
  fn requeue(&mut self, piece_idx: u32, offset: u32) {
    if !self.pieces_in_slot.contains(&piece_idx)
      || has_piece(&self.verified_pieces, piece_idx as usize)
    {
      return;
    }
    let key = (piece_idx, offset);
    if self.received.contains(&key) || self.requested.contains_key(&key) {
      return;
    }
    let offs = self.pending.entry(piece_idx).or_default();
    if !offs.contains(&offset) {
      offs.push(offset);
    }
  }

  fn release_requests_of(&mut self, peer: &PeerHandle) {
    let keys: Vec<(u32, u32)> = self
      .requested
      .iter()
      .filter(|(_, (owner, _))| owner == peer)
      .map(|(k, _)| *k)
      .collect();
    for key in keys {
      self.requested.remove(&key);
      self.requeue(key.0, key.1);
    }
    self.per_peer.remove(peer);
  }

  // requests nobody answered in time go back into the pool
  fn release_stale(&mut self) {
    let now = Instant::now();
    let stale: Vec<(u32, u32)> = self
      .requested
      .iter()
      .filter(|(_, (_, sent_at))| now.duration_since(*sent_at) >= REQUEST_TIMEOUT)
      .map(|(k, _)| *k)
      .collect();

    for key in stale {
      if let Some((peer, _)) = self.requested.remove(&key) {
        self.dec_inflight(&peer);
      }
      self.requeue(key.0, key.1);
    }
  }

  fn update_rates(&mut self) {
    let now = Instant::now();
    let secs = now.duration_since(self.last_sweep).as_secs_f64().max(0.001);
    self.last_sweep = now;

    for r in self.peer_rate.values_mut() {
      let inst = r.blocks as f64 / secs;
      r.per_sec = if r.per_sec == 0.0 {
        inst
      } else {
        r.per_sec * 0.5 + inst * 0.5
      };
      r.blocks = 0;
    }
  }

  /// ~4 seconds of data per peer: slow peers can't hoard blocks
  fn in_flight_limit(&self, peer: &PeerHandle) -> usize {
    let per_sec = self.peer_rate.get(peer).map_or(0.0, |r| r.per_sec);
    ((per_sec * TARGET_QUEUE_SECS).ceil() as usize).clamp(MIN_DEPTH, MAX_DEPTH)
  }

  fn schedule(&mut self) {
    let peers: Vec<PeerHandle> = self.active_peers.iter().cloned().collect();
    for peer in &peers {
      self.request_peer(peer);
    }
  }

  // fill ONE peer's pipeline up to its adaptive depth
  fn request_peer(&mut self, peer: &PeerHandle) {
    if !self.unchoked.contains(peer) {
      return;
    }

    let limit = self.in_flight_limit(peer);
    let mut inflight = *self.per_peer.get(peer).unwrap_or(&0);
    let has = peer.available_pieces.read().unwrap();

    while inflight < limit {
      let Some((piece, offset)) = self.next_for(&has) else {
        break;
      };

      let length = self.geometry.get_block_length(piece, offset);
      let cmd = PeerCommand::Request(BlockMeta {
        piece_idx: piece,
        offset,
        length,
      });

      match peer.cmd_sender.try_send(cmd) {
        Ok(()) => {
          self
            .requested
            .insert((piece, offset), (peer.clone(), Instant::now()));
          inflight += 1;
        }
        Err(TrySendError::Full(_)) => {
          self.requeue(piece, offset); // peer is busy, try next round
          break;
        }
        Err(TrySendError::Closed(_)) => {
          self.requeue(piece, offset);
          self.unchoked.remove(peer);
          self.release_requests_of(peer);
          return;
        }
      }
    }

    self.per_peer.insert(peer.clone(), inflight);
  }

  // next block this peer can serve: pieces already open first, then open a new one
  fn next_for(&mut self, has: &Vec<u8>) -> Option<(u32, u32)> {
    loop {
      // 1) blocks from pieces that already hold a slot (lowest piece first)
      let mut found: Option<(u32, u32)> = None;
      for (idx, offs) in self.pending.iter_mut() {
        if offs.is_empty() || !Self::peer_has(has, *idx) {
          continue;
        }
        found = Some((*idx, offs.pop().unwrap()));
        break;
      }
      if let Some((idx, offset)) = found {
        if self.pending.get(&idx).is_some_and(|o| o.is_empty()) {
          self.pending.remove(&idx);
        }
        return Some((idx, offset));
      }

      // 2) open a brand new piece this peer has
      let piece = self.pick_new_piece(has)?;
      self.open_piece(piece);
    }
  }

  fn open_piece(&mut self, piece: u32) {
    self.pieces_in_slot.insert(piece);
    // reversed so pop() hands out the lowest offset first
    let offs: Vec<u32> = (0..self.geometry.get_block_count(piece))
      .rev()
      .map(|i| i * BLOCK_SIZE as u32)
      .collect();
    self.pending.insert(piece, offs);
  }

  // lowest index this peer has, rarest-first is the upgrade for later
  fn pick_new_piece(&mut self, has: &Vec<u8>) -> Option<u32> {
    if self.pieces_in_slot.len() as u16 >= self.max_active_pieces {
      return None;
    }

    while has_piece(&self.verified_pieces, self.first_unverified as usize) {
      self.first_unverified += 1;
    }

    let now = Instant::now();
    (self.first_unverified..self.total_pieces).find(|p| {
      !has_piece(&self.verified_pieces, *p as usize)
        && !self.pieces_in_slot.contains(p)
        && self.cooldown.get(p).map_or(true, |t| *t <= now)
        && Self::peer_has(has, *p)
    })
  }

  // peers start with an empty bitfield, so guard the index before has_piece
  fn peer_has(bits: &Vec<u8>, piece: u32) -> bool {
    (piece as usize) / 8 < bits.len() && has_piece(bits, piece as usize)
  }
}
