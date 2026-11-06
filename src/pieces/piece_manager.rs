use std::{collections::HashMap, vec};

use bytes::{Bytes, BytesMut};
use tokio::{
  sync::mpsc::{self, UnboundedSender},
  task::JoinHandle,
};

use crate::{
  pieces::piece_validator::ValidatorCommand,
  storage_manager::{BlockMeta, StorageCommand, StorageMessage, StorageResult},
};

use crate::{
  event::event::TorrentEvent::{self},
  file_worker::{
    FileLayout::{MultiFileLayout, SingleFileLayout},
    TorrentMetadata,
  },
  peer_protocol::BlockRequest,
  peers::{
    client::BLOCK_SIZE,
    peer::{PeerCommand, PeerHandle},
  },
  pieces::{bitfield::add_piece, piece_geometry::PieceGeometry},
  recon::Recon,
  stats::format_bytes,
};

#[derive(Clone)]
pub struct Piece {
  pub piece_idx: u32,
  pub offset: u32,
  pub block: Bytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PieceState {
  Checking, // <- new
  Complete,
  Downloading,
  Verifying,
  Writing,
}

const MAX_CONCURRENT_CHECKS: usize = 4;
const MAX_WRITE_RETRIES: u8 = 3;
pub struct PieceManager {
  total_length: u64,
  downloaded_completed: bool,
  blocks_per_piece: u16,
  downloaded_piece_counter: u32,
  total_pieces: u32,

  incoming_cmd: mpsc::Receiver<PieceManagerMessage>,

  geometry: PieceGeometry,
  piece_hashes: Bytes,
  verified_pieces: Vec<u8>,
  blocks: HashMap<(u32, u32), Bytes>,

  storage_tx: UnboundedSender<StorageMessage>,

  validator_tx: UnboundedSender<ValidatorCommand>,

  recon: Recon<TorrentEvent>,
  senders: HashMap<u32, HashMap<u64, PeerHandle>>,

  fail_streak: u32,

  states: Vec<PieceState>,

  check_cursor: u32,
  checks_in_flight: usize,
  write_retries: HashMap<u32, u8>,
}

pub enum PieceManagerMessage {
  IncomingBlock(Piece),
  IncomingRequest(IncomingRequest),
  ValidatorResult {
    piece_idx: u32,
    success: bool,
    data: Bytes,
  },
  StorageResult(StorageResult),
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

    storage_tx: UnboundedSender<StorageMessage>,

    validator_tx: UnboundedSender<ValidatorCommand>,

    recon: Recon<TorrentEvent>,

    incoming_cmd: mpsc::Receiver<PieceManagerMessage>,
  ) -> Self {
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
    let geometry = PieceGeometry::new(total_length, piece_length);

    println!("Piece Length: {}", format_bytes(piece_length as u64));
    println!("Piece count: {}", total_pieces);

    let manager: PieceManager = PieceManager {
      total_length,
      downloaded_completed: false,
      blocks_per_piece,
      downloaded_piece_counter: 0,
      total_pieces,
      incoming_cmd,
      validator_tx,
      storage_tx,
      geometry,
      piece_hashes,
      verified_pieces,
      blocks: HashMap::new(),
      recon,
      senders: HashMap::new(),
      fail_streak: 0,
      states: vec![PieceState::Downloading; total_pieces as usize],
      check_cursor: 0,
      checks_in_flight: 0,
      write_retries: HashMap::new(),
    };

    return manager;
  }

  fn spawn(mut self) -> JoinHandle<()> {
    tokio::spawn(async move {
      loop {
        match self.incoming_cmd.recv().await {
          Some(cmd) => match cmd {
            PieceManagerMessage::IncomingRequest(IncomingRequest {
              incoming_request,
              outgoing_sender,
            }) => {
              println!("Received an incoming request");
            }

            PieceManagerMessage::StorageResult(StorageResult::ReadBlock { meta, data }) => {
              if let Some(PieceState::Checking) = self.state_of(meta.piece_idx) {
                match data {
                  Ok(bytes) => {
                    let _ = self.validator_tx.send(ValidatorCommand::VerifyPiece {
                      piece_idx: meta.piece_idx,
                      data: bytes,
                      expected_hash: self.expected_hash(meta.piece_idx),
                    });
                  }
                  Err(_) => self.finish_check(meta.piece_idx, false), // unreadable = not done
                }
              } else {
              }
            }

            PieceManagerMessage::IncomingBlock(Piece {
              piece_idx,
              offset,
              block,
            }) => {
              let Some(PieceState::Downloading) = self.state_of(piece_idx) else {
                continue;
              };

              // reject garbage from peers 🛡️
              if offset % BLOCK_SIZE as u32 != 0
                || offset >= self.geometry.get_piece_length(piece_idx)
                || block.len() as u32 != self.geometry.get_block_length(piece_idx, offset)
              {
                continue;
              }

              let key = (piece_idx, offset);

              if self.blocks.contains_key(&key) {
                continue; // duplicate, keep the first
              }

              self.blocks.insert(key, block);

              if self.have_all_blocks(piece_idx) {
                let assembled_piece = self.assemble_piece(piece_idx);
                self.clear_piece_blocks(piece_idx); // frees memory, no leak 🧠
                self.states[piece_idx as usize] = PieceState::Verifying;

                let _ = self.validator_tx.send(ValidatorCommand::VerifyPiece {
                  piece_idx,
                  data: assembled_piece,
                  expected_hash: self.expected_hash(piece_idx),
                });
              }
            }

            PieceManagerMessage::StorageResult(StorageResult::WriteResult {
              piece_idx,
              result,
            }) => {
              let Some(PieceState::Writing) = self.state_of(piece_idx) else {
                continue;
              };

              match result {
                Ok(_) => {
                  self.write_retries.remove(&piece_idx);
                  self.states[piece_idx as usize] = PieceState::Complete;
                  add_piece(&mut self.verified_pieces, piece_idx as usize);
                  self.downloaded_piece_counter += 1;
                  self.recon.announce(TorrentEvent::PieceComplete(
                    piece_idx,
                    self.geometry.get_piece_length(piece_idx),
                  ));
                  self.complete_download_handler();
                }
                Err(data) => {
                  let tries = self.write_retries.entry(piece_idx).or_insert(0);
                  *tries += 1;
                  if *tries > MAX_WRITE_RETRIES {
                    eprintln!("giving up writing piece {piece_idx}");
                    continue;
                  }
                  let _ =
                    self
                      .storage_tx
                      .send(StorageMessage::Command(StorageCommand::WritePiece {
                        piece_idx,
                        data,
                      }));
                }
              }
            }

            PieceManagerMessage::ValidatorResult {
              piece_idx,
              success,
              data,
            } => {
              let Some(state) = self.state_of(piece_idx) else {
                continue;
              };

              match (state, success) {
                (PieceState::Checking, ok) => self.finish_check(piece_idx, ok),

                (PieceState::Verifying, true) => {
                  self.states[piece_idx as usize] = PieceState::Writing;
                  let _ =
                    self
                      .storage_tx
                      .send(StorageMessage::Command(StorageCommand::WritePiece {
                        piece_idx,
                        data,
                      }));
                }

                (PieceState::Verifying, false) => {
                  self.states[piece_idx as usize] = PieceState::Downloading;
                  self.clear_piece_blocks(piece_idx);
                  self.recon.announce(TorrentEvent::PieceFail(piece_idx)); // see note 1
                }

                _ => continue,
              }
            }
          },
          None => break,
        }
      }
    })
  }

  pub async fn run(mut self) {
    self.states.fill(PieceState::Checking);
    println!("Checking for existing files ⏳");
    self.start_next_checks();
    self.spawn();
  }

  fn state_of(&self, piece_idx: u32) -> Option<PieceState> {
    self.states.get(piece_idx as usize).copied() // no panic on a bad index
  }

  fn expected_hash(&self, piece_idx: u32) -> Bytes {
    let start = piece_idx as usize * 20;
    self.piece_hashes.slice(start..start + 20)
  }

  fn start_next_checks(&mut self) {
    while self.checks_in_flight < MAX_CONCURRENT_CHECKS && self.check_cursor < self.total_pieces {
      let piece_idx = self.check_cursor;
      self.check_cursor += 1;
      self.checks_in_flight += 1;

      let _ = self
        .storage_tx
        .send(StorageMessage::Command(StorageCommand::ReadPiece(
          BlockMeta {
            piece_idx,
            offset: 0,
            length: self.geometry.get_piece_length(piece_idx),
          },
        )));
    }
  }

  fn finish_check(&mut self, piece_idx: u32, ok: bool) {
    self.checks_in_flight = self.checks_in_flight.saturating_sub(1);

    if ok {
      self.states[piece_idx as usize] = PieceState::Complete;
      add_piece(&mut self.verified_pieces, piece_idx as usize);
      self.downloaded_piece_counter += 1;
      self.recon.announce(TorrentEvent::Exists(
        piece_idx,
        self.geometry.get_piece_length(piece_idx),
      ));
    } else {
      self.states[piece_idx as usize] = PieceState::Downloading;
    }

    self.start_next_checks();

    if self.checks_in_flight == 0 && self.check_cursor >= self.total_pieces {
      self.complete_download_handler(); // everything was already on disk
    }
  }

  fn clear_piece_blocks(&mut self, piece_idx: u32) {
    let block_count = self.geometry.get_block_count(piece_idx);

    for i in 0..block_count {
      let offset = i * BLOCK_SIZE as u32;
      self.blocks.remove(&(piece_idx, offset));
    }
  }

  fn assemble_piece(&self, piece_idx: u32) -> Bytes {
    let mut buf = BytesMut::with_capacity(self.geometry.get_piece_length(piece_idx) as usize);
    let blocks = self.geometry.get_block_count(piece_idx);

    for block_idx in 0..blocks {
      let key = (piece_idx, block_idx * BLOCK_SIZE as u32);
      let block = self.blocks.get(&key).unwrap();

      buf.extend_from_slice(block);
    }

    buf.freeze()
  }

  fn have_all_blocks(&self, piece_idx: u32) -> bool {
    let num_blocks = self.geometry.get_block_count(piece_idx);

    for block_idx in 0..num_blocks {
      let key = (piece_idx, block_idx * BLOCK_SIZE as u32);
      if !self.blocks.contains_key(&key) {
        return false;
      }
    }

    true
  }

  // pub fn all_offsets(&self, piece_idx: u32) -> Option<Vec<u32>> {
  //   if has_piece(&self.verified_pieces, piece_idx as usize) {
  //     return None;
  //   }
  //   let mut offs = Vec::new();

  //   for block_idx in (0..self.geometry.get_block_count(piece_idx)).rev() {
  //     if !self
  //       .blocks
  //       .contains_key(&(piece_idx, block_idx * BLOCK_SIZE as u32))
  //     {
  //       offs.push(block_idx * BLOCK_SIZE as u32);
  //     }
  //   }

  //   Some(offs)
  // }

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

  fn settle_suspects(&self, _piece_idx: u32, piece_block: Bytes) {
    let _ = piece_block;
  }

  fn blame_peers(&self, piece_idx: u32) {
    let _ = piece_idx;
  }

  pub fn record_block(&mut self, piece_idx: u32, offset: u32, peer: PeerHandle) {
    let m = self.senders.entry(piece_idx).or_default();
    m.insert(offset as u64, peer);
  }

  // async fn get_block(
  //   &mut self,
  //   &BlockRequest {
  //     piece_idx: piece_idx,
  //     offset,
  //     length,
  //   }: &BlockRequest,
  // ) -> Option<Bytes> {
  //   if piece_idx >= self.total_pieces || length > 32 * 1024 {
  //     return None;
  //   }

  //   if !has_piece(&self.verified_pieces, piece_idx as usize) {
  //     return None;
  //   }

  //   match self
  //     .storage_manager
  //     .read_block(piece_idx, offset, length)
  //     .await
  //   {
  //     Ok(block) => return Some(block),
  //     Err(_) => return None,
  //   }
  // }
}
