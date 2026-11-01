use std::{
  collections::HashSet,
  sync::{Arc, RwLock, mpsc},
};

use crate::pieces::piece_manager::PieceManagerHandle;
use crate::{event::TorrentEvent, peers::peer::PeerHandle, recon::Recon};

pub struct PieceInfo {
  piece_idx: u32,
  offset: u32,
}

enum SchedulerEvent {
  Connect(PeerHandle),
  Unchoke(PeerHandle),
  Choke(PeerHandle),
  Have(PeerHandle),
  Bitfield,
  BlockReceive(PieceInfo),
  Disconnect(PeerHandle),
}

pub struct Scheduler {
  max_active_pieces: u16,
  pieces_in_slot: HashSet<u32>,

  active_peers: Arc<RwLock<HashSet<PeerHandle>>>,
  verified_pieces: Arc<RwLock<HashSet<u32>>>,

  event_queue: mpsc::Receiver<SchedulerEvent>,

  recon: Recon<TorrentEvent>,

  blocks_per_piece: u16,

  piece_manager: PieceManagerHandle,
}

impl Scheduler {}
