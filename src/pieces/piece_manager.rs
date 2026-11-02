use std::collections::HashMap;

use bytes::{Bytes, BytesMut};
use tokio::{
  sync::mpsc::{self, Sender},
  task::JoinHandle,
};

use crate::{
  event::event::TorrentEvent::{self},
  peer_protocol::BlockRequest,
  peers::{
    client::BLOCK_SIZE,
    peer::{PeerCommand, PeerHandle},
  },
  pieces::{
    bitfield::{add_piece, has_piece},
    piece_geometry::PieceGeometry,
    verify_piece::verify_piece,
  },
  recon::Recon,
  stats::format_bytes,
  storage_manager::{
    FileLayout::{MultiFileLayout, SingleFileLayout},
    StorageManager, TorrentMetadata,
  },
};

#[derive(Clone)]
pub struct Piece {
  pub piece_idx: u32,
  pub offset: u32,
  pub block: Bytes,
}

pub struct PieceManager {
  total_length: u64,
  downloaded_completed: bool,
  blocks_per_piece: u16,
  downloaded_piece_counter: u32,
  total_pieces: u32,

  incoming_cmd: mpsc::Receiver<PieceManagerMessage>,

  validator: PieceGeometry,
  piece_hashes: Bytes,
  verified_pieces: Vec<u8>,
  blocks: HashMap<(u32, u32), Bytes>,

  storage_manager: StorageManager,
  recon: Recon<TorrentEvent>,
  senders: HashMap<u32, HashMap<u64, PeerHandle>>,

  fail_streak: u32,
}

pub enum PieceManagerMessage {
  IncomingBlock(Piece),
  IncomingRequest(IncomingRequest),
}

#[derive(Clone)]
pub struct IncomingRequest {
  pub incoming_request: BlockRequest,
  pub outgoing_sender: mpsc::Sender<PeerCommand>,
}

impl IncomingRequest {
  pub async fn send_block(self, command: PeerCommand) {
    let _ = self.outgoing_sender.send(command).await;
  }
}

impl PieceManager {
  pub fn new(
    meta: TorrentMetadata,
    storage_manager: StorageManager,
    recon: Recon<TorrentEvent>,
  ) -> (Self, Sender<PieceManagerMessage>) {
    let TorrentMetadata {
      piece_hashes,
      piece_length,
      ..
    } = meta;

    let total_length = match meta.layout {
      SingleFileLayout { length } => length,
      MultiFileLayout { files } => files.iter().map(|file| file.length).sum(),
    };

    let total_pieces = total_length.div_ceil(piece_length as u64) as u32;
    let verified_pieces: Vec<u8> = vec![0u8; ((total_pieces + 7) >> 3) as usize];
    let blocks_per_piece = piece_length.div_ceil(BLOCK_SIZE as u32) as u16;
    let validator = PieceGeometry::new(total_length, piece_length);

    println!("Piece length: {}", format_bytes(piece_length as u64));
    println!("Piece count: {}", format_bytes(total_pieces as u64));

    let (cmd_sender, incoming_cmd) = tokio::sync::mpsc::channel::<PieceManagerMessage>(200);

    let manager: PieceManager = PieceManager {
      total_length,
      downloaded_completed: false,
      blocks_per_piece,
      downloaded_piece_counter: 0,
      total_pieces,
      incoming_cmd,
      validator,
      piece_hashes,
      verified_pieces,
      blocks: HashMap::new(),
      storage_manager,
      recon,
      senders: HashMap::new(),
      fail_streak: 0,
    };

    return (manager, cmd_sender);
  }

  pub fn run(mut self) {
    self.complete_download_handler();
    self.spawn();
  }

  fn spawn(mut self) -> JoinHandle<()> {
    tokio::spawn(async move {
      loop {
        match self.incoming_cmd.recv().await {
          Some(cmd) => match cmd {
            PieceManagerMessage::IncomingBlock(piece) => {
              self.receive_block(piece).await;
            }
            PieceManagerMessage::IncomingRequest(IncomingRequest {
              incoming_request,
              outgoing_sender,
            }) => {
              let Some(block) = self.get_block(&incoming_request).await else {
                continue;
              };

              let block_length = block.len() as u32;
              outgoing_sender
                .send(PeerCommand::SendBlock(incoming_request, block))
                .await;

              self.recon.announce(TorrentEvent::BlockUpload(block_length));
            }
          },
          None => break,
        }
      }
    })
  }

  fn clear_piece_blocks(&mut self, piece_idx: u32) {
    let block_count = self.validator.get_block_count(piece_idx);

    for i in 0..block_count {
      let offset = i * BLOCK_SIZE as u32;
      self.blocks.remove(&(piece_idx, offset));
    }
  }

  pub async fn verify_existing_pieces() {}

  pub fn get_needed_blocks(&self, piece_idx: u32) -> Option<Vec<BlockRequest>> {
    if has_piece(&self.verified_pieces, piece_idx as usize) {
      return None;
    };

    let num_blocks = self.validator.get_block_count(piece_idx);
    let mut needed_blocks = Vec::new();

    for i in 0..num_blocks {
      let offset = i * BLOCK_SIZE as u32;
      let length = self.validator.get_block_length(piece_idx, offset);

      let key = (piece_idx, offset);

      if !self.blocks.contains_key(&key) {
        needed_blocks.push(BlockRequest {
          piece_idx,
          offset,
          length,
        });
      }
    }

    Some(needed_blocks)
  }

  fn assemble_piece(&self, piece_idx: u32) -> Bytes {
    let mut buf = BytesMut::with_capacity(self.validator.get_piece_length(piece_idx) as usize);
    let blocks = self.validator.get_block_count(piece_idx);

    for block_idx in 0..blocks {
      let key = (piece_idx, block_idx * BLOCK_SIZE as u32);
      let block = self.blocks.get(&key).unwrap();

      buf.extend_from_slice(block);
    }

    buf.freeze()
  }

  fn is_complete_piece(&self, piece_idx: u32) -> bool {
    let num_blocks = self.validator.get_block_count(piece_idx);

    for block_idx in 0..num_blocks {
      let key = (piece_idx, block_idx * BLOCK_SIZE as u32);
      if !self.blocks.contains_key(&key) {
        return false;
      }
    }

    true
  }

  pub fn all_offsets(&self, piece_idx: u32) -> Option<Vec<u32>> {
    if has_piece(&self.verified_pieces, piece_idx as usize) {
      return None;
    }
    let mut offs = Vec::new();

    for block_idx in (0..self.validator.get_block_count(piece_idx)).rev() {
      if !self
        .blocks
        .contains_key(&(piece_idx, block_idx * BLOCK_SIZE as u32))
      {
        offs.push(block_idx * BLOCK_SIZE as u32);
      }
    }

    Some(offs)
  }

  async fn receive_block(
    &mut self,
    Piece {
      piece_idx,
      offset,
      block,
    }: Piece,
    // peer: PeerHandle,
  ) {
    if has_piece(&self.verified_pieces, piece_idx as usize) {
      return;
    }

    if !self
      .validator
      .is_valid_block(piece_idx, offset, block.len() as u32)
    {
      println!("Invalid piece in receive_block");
      return;
    }

    let block_key = (piece_idx, offset);
    if self.blocks.contains_key(&block_key) {
      return;
    }

    self.blocks.insert(block_key, block.clone());
    // self.record_block(piece_idx, offset, peer);

    self.recon.announce(TorrentEvent::BlockReceive(Piece {
      piece_idx,
      offset,
      block,
    }));

    if !self.is_complete_piece(piece_idx) {
      return;
    }

    let assembled = self.assemble_piece(piece_idx);

    let start = piece_idx as usize * 20;
    let end = start + 20;

    if !verify_piece(assembled.clone(), &self.piece_hashes.slice(start..end)).await {
      self.blame_peers(piece_idx);
      self.clear_piece_blocks(piece_idx);
      self.recon.announce(TorrentEvent::PieceFail(piece_idx));
      return;
    }

    self.fail_streak = 0;
    self.settle_suspects(piece_idx, assembled.clone());

    let piece_length = assembled.len() as u32;

    self.storage_manager.write_piece(piece_idx, assembled).await;

    if !has_piece(&self.verified_pieces, piece_idx as usize) {
      self.downloaded_piece_counter += 1;
      add_piece(&mut self.verified_pieces, piece_idx as usize);
    }

    self.senders.remove(&piece_idx);
    self.clear_piece_blocks(piece_idx);

    self
      .recon
      .announce(TorrentEvent::PieceComplete(piece_idx, piece_length));

    self.complete_download_handler();
  }

  fn complete_download_handler(&mut self) {
    if self.downloaded_completed {
      return;
    }

    if self.downloaded_piece_counter != self.total_pieces {
      return;
    }

    self.downloaded_completed = true;

    self.blocks.clear();

    self.recon.announce(TorrentEvent::DownloadComplete);
  }

  fn settle_suspects(&self, piece_idx: u32, piece_block: Bytes) {}

  fn blame_peers(&self, piece_idx: u32) {}

  pub fn record_block(&mut self, piece_idx: u32, offset: u32, peer: PeerHandle) {
    let m = self.senders.entry(piece_idx).or_default();
    m.insert(offset as u64, peer);
  }

  async fn get_block(
    &mut self,
    &BlockRequest {
      piece_idx: piece_idx,
      offset,
      length,
    }: &BlockRequest,
  ) -> Option<Bytes> {
    if piece_idx > self.total_pieces || length > 32 * 1024 {
      return None;
    }

    if !has_piece(&self.verified_pieces, piece_idx as usize) {
      return None;
    }

    match self
      .storage_manager
      .read_block(piece_idx, offset, length)
      .await
    {
      Ok(block) => return Some(block),
      Err(_) => return None,
    }
  }
}
