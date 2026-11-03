use std::{
  collections::{HashMap, HashSet},
  sync::{Arc, RwLock},
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
  peer_protocol::BlockRequest,
  peers::{
    client::BLOCK_SIZE,
    peer::{PeerCommand, PeerHandle},
  },
  pieces::{bitfield::has_piece, piece_geometry::PieceGeometry},
};

const MAX_INFLIGHT_PER_PEER: usize = 160;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

pub struct Scheduler {
  max_active_pieces: u16,
  pieces_in_slot: HashSet<u32>, // pieces we are currently downloading

  active_peers: Arc<RwLock<HashSet<PeerHandle>>>, // shared with PeerManager
  unchoked: HashSet<PeerHandle>,
  verified_pieces: HashSet<u32>,

  requested: HashMap<(u32, u32), (PeerHandle, Instant)>, // (piece, offset) -> who, when
  received: HashSet<(u32, u32)>,

  event_queue: UnboundedReceiver<TorrentEvent>,
  total_pieces: u32,
  geometry: PieceGeometry,
}

impl Scheduler {
  pub fn new(
    active_peers: Arc<RwLock<HashSet<PeerHandle>>>,
    event_queue: UnboundedReceiver<TorrentEvent>,
    total_length: u64,
    piece_length: u32,
  ) -> Self {
    Self {
      max_active_pieces: 80,
      pieces_in_slot: HashSet::new(),
      active_peers,
      unchoked: HashSet::new(),
      verified_pieces: HashSet::new(),
      requested: HashMap::new(),
      received: HashSet::new(),
      event_queue,
      total_pieces: total_length.div_ceil(piece_length as u64) as u32,
      geometry: PieceGeometry::new(total_length, piece_length),
    }
  }

  pub fn run(mut self) -> JoinHandle<()> {
    tokio::spawn(async move {
      let mut tick = interval(Duration::from_secs(2));
      loop {
        select! {
          event = self.event_queue.recv() => match event {
            Some(TorrentEvent::DownloadComplete) | None => break,
            Some(event) => self.reducer(event),
          },
          _ = tick.tick() => self.release_stale(),
        }
        self.schedule(); // after every wakeup, try to fill the pipes
      }
    })
  }

  //mutates state
  fn reducer(&mut self, event: TorrentEvent) {
    match event {
      TorrentEvent::Unchoke(peer) => {
        self.unchoked.insert(peer);
      }
      TorrentEvent::Choke(peer) | TorrentEvent::Disconnect(peer) => {
        // a choked peer drops our requests (your Peer clears pending_requests too)
        self.unchoked.remove(&peer);
        self.release_requests_of(&peer);
      }
      TorrentEvent::BlockReceive(piece) => {
        if !self.pieces_in_slot.contains(&piece.piece_idx) {
          return; // late block for a piece we're no longer working on
        }
        let key = (piece.piece_idx, piece.offset);
        self.requested.remove(&key);
        self.received.insert(key);
      }
      TorrentEvent::PieceComplete(piece_idx, _len) => {
        self.pieces_in_slot.remove(&piece_idx);
        self.verified_pieces.insert(piece_idx);
        self.forget_piece(piece_idx);
      }
      TorrentEvent::PieceFail(piece_idx) => {
        // stays in the slot, so its blocks simply get requested again
        self.forget_piece(piece_idx);
      }

      TorrentEvent::Exists(idx, _len) => {
        self.verified_pieces.insert(idx);
      }
      _ => {}
    }
  }

  fn forget_piece(&mut self, piece_idx: u32) {
    self.received.retain(|(p, _)| *p != piece_idx);
    self.requested.retain(|(p, _), _| *p != piece_idx);
  }

  fn release_requests_of(&mut self, peer: &PeerHandle) {
    self.requested.retain(|_, (owner, _)| owner != peer);
  }

  // requests nobody answered in time go back into the pool
  fn release_stale(&mut self) {
    let now = Instant::now();
    self
      .requested
      .retain(|_, (_, sent_at)| now.duration_since(*sent_at) < REQUEST_TIMEOUT);
  }

  fn schedule(&mut self) {
    let peers: Vec<PeerHandle> = self.active_peers.read().unwrap().iter().cloned().collect();

    // 1. open piece slots until we hit the cap
    while (self.pieces_in_slot.len() as u16) < self.max_active_pieces {
      match self.pick_new_piece(&peers) {
        Some(piece) => {
          self.pieces_in_slot.insert(piece);
        }
        None => break,
      }
    }

    // 2. fill each unchoked peer's pipeline from the active pieces it has
    let mut inflight: HashMap<&PeerHandle, usize> = HashMap::new();
    for (peer, _) in self.requested.values() {
      *inflight.entry(peer).or_default() += 1;
    }
    let budgets: Vec<usize> = peers
      .iter()
      .map(|p| MAX_INFLIGHT_PER_PEER.saturating_sub(*inflight.get(p).unwrap_or(&0)))
      .collect();
    drop(inflight); // releases the borrow on self.requested

    let slots: Vec<u32> = self.pieces_in_slot.iter().copied().collect();
    let mut dead: Vec<PeerHandle> = Vec::new();

    for (peer, mut budget) in peers.iter().zip(budgets) {
      if budget == 0 || !self.unchoked.contains(peer) {
        continue;
      }
      let has = peer.available_pieces.read().unwrap();

      'pieces: for &piece in &slots {
        if !Self::peer_has(&has, piece) {
          continue;
        }
        for i in 0..self.geometry.get_block_count(piece) {
          if budget == 0 {
            break 'pieces;
          }
          let offset = i * BLOCK_SIZE as u32;
          let key = (piece, offset);
          if self.requested.contains_key(&key) || self.received.contains(&key) {
            continue;
          }

          let length = self.geometry.get_block_length(piece, offset);
          let cmd = PeerCommand::Request(BlockRequest {
            piece_idx: piece,
            offset,
            length,
          });

          match peer.cmd_sender.try_send(cmd) {
            Ok(()) => {
              self.requested.insert(key, (peer.clone(), Instant::now()));
              budget -= 1;
            }
            Err(TrySendError::Full(_)) => break 'pieces, // peer is busy, try next round
            Err(TrySendError::Closed(_)) => {
              dead.push(peer.clone());
              break 'pieces;
            }
          }
        }
      }
    }

    for peer in dead {
      self.unchoked.remove(&peer);
      self.release_requests_of(&peer);
    }
  }

  // lowest index for now, rarest-first is the upgrade for later
  fn pick_new_piece(&self, peers: &[PeerHandle]) -> Option<u32> {
    (0..self.total_pieces).find(|p| {
      !self.verified_pieces.contains(p)
        && !self.pieces_in_slot.contains(p)
        && peers.iter().any(|peer| {
          self.unchoked.contains(peer) && Self::peer_has(&peer.available_pieces.read().unwrap(), *p)
        })
    })
  }

  // peers start with an empty bitfield, so guard the index before has_piece
  fn peer_has(bits: &Vec<u8>, piece: u32) -> bool {
    (piece as usize) / 8 < bits.len() && has_piece(bits, piece as usize)
  }
}
