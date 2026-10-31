use std::{collections::HashSet, sync::Arc};

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::{
  event::TorrentEvent, pieces::piece_validator::PieceValidator, recon::Recon,
  storage_manager::StorageManager,
};

#[derive(Clone)]
pub struct Piece {
  piece_idx: u32,
  offset: u32,
  block: Bytes,
}

enum PieceCommand {}

pub struct PieceManagerHandle {
  cmd_sender: mpsc::Receiver<PieceCommand>,
}

pub struct PieceManager {
  total_length: u64,
  downloaded_completed: bool,
  blocks_per_piece: u16,

  incoming_cmd: mpsc::Receiver<PieceCommand>,

  validator: PieceValidator,
  hashes: Bytes,
  verified_pieces: Arc<HashSet<u32>>,

  storage: StorageManager,
  recon: Recon<TorrentEvent>,
}
