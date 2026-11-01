use std::{collections::HashSet, sync::Arc};

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::{
  event::event::TorrentEvent, peer_protocol::BlockRequest, pieces::piece_geometry::PieceGeometry,
  recon::Recon, storage_manager::StorageManager,
};

#[derive(Clone)]
pub struct Piece {
  pub piece_idx: u32,
  pub offset: u32,
  pub block: Bytes,
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

  validator: PieceGeometry,
  hashes: Bytes,
  verified_pieces: Arc<HashSet<u32>>,

  storage: StorageManager,
  recon: Recon<TorrentEvent>,
}

pub enum PieceManagerMessage {
  IncomingBlock(Piece),
  IncomingRequest(IncomingRequest),
}

#[derive(Clone)]
pub struct IncomingRequest {
  pub incoming_request: BlockRequest,
  pub outgoing_sender: mpsc::Sender<Bytes>,
}

impl IncomingRequest {
  pub async fn send_block(self, data: Bytes) {
    let _ = self.outgoing_sender.send(data).await;
  }
}
