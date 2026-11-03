use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use tokio::select;
use tokio::sync::{self, mpsc};
use tokio::task::JoinSet;

use crate::{file_worker::FileWorker, pieces::piece_manager::PieceManagerMessage};

const MAX_CONCURRENCY_WRITES: usize = 4;
const MAX_CONCURRENCY_READS: usize = 20;

pub struct StorageManager {
  worker: Arc<FileWorker>,
  command_rx: mpsc::Receiver<StorageCommand>,
}

pub enum StorageCommand {
  WritePiece {
    piece_idx: u32,
    data: Bytes,
    reply: sync::oneshot::Sender<PieceManagerMessage>,
  },
  ReadPiece {
    meta: BlockMeta,
    reply: sync::oneshot::Sender<PieceManagerMessage>,
  },
}

#[derive(Clone)]
pub struct BlockMeta {
  piece_idx: u32,
  offset: u32,
  length: u32,
}

#[derive(Clone)]
pub enum StorageResult {
  ReadBlock {
    meta: BlockMeta,
    data: Result<Bytes, String>,
  },
  WriteResult {
    piece_idx: u32,
    result: Result<(), String>,
  },
}

impl StorageManager {
  fn new(worker: FileWorker) -> (Self, mpsc::Sender<StorageCommand>) {
    let (command_tx, command_rx) = mpsc::channel::<StorageCommand>(200);
    let manager = StorageManager {
      worker: Arc::new(worker),
      command_rx,
    };

    (manager, command_tx)
  }

  pub fn run(mut self) {
    let mut writes: JoinSet<std::io::Result<()>> = JoinSet::new();
    let mut reads: JoinSet<std::io::Result<Bytes>> = JoinSet::new();

    let mut write_queue: VecDeque<StorageCommand> = VecDeque::new();
    let mut read_queue: VecDeque<StorageCommand> = VecDeque::new();

    tokio::spawn(async move {
      loop {
        select! {
          Some(command) = self.command_rx.recv() =>  {
            match command {
                StorageCommand::WritePiece { piece_idx, data, reply } => write_queue.push_back(StorageCommand::WritePiece { piece_idx, data, reply }),
                StorageCommand::ReadPiece { meta, reply } => read_queue.push_back(StorageCommand::ReadPiece { meta, reply }),
            }
          },

          Some(_) = writes.join_next(), if !writes.is_empty() => {
          },

          Some(_) = reads.join_next(), if !reads.is_empty() => {},

          else => break
        }

        while writes.len() < MAX_CONCURRENCY_WRITES {
          let Some(StorageCommand::WritePiece {
            piece_idx,
            data,
            reply,
          }) = write_queue.pop_front()
          else {
            return;
          };

          let worker = self.worker.clone();

          writes.spawn_blocking(move || {
            let result = worker.write_piece(piece_idx, data);

            let message = PieceManagerMessage::StorageReply(StorageResult::WriteResult {
              piece_idx,
              result: result.as_ref().map(|_| ()).map_err(|err| err.to_string()),
            });

            let _ = reply.send(message);

            result
          });
        }

        while reads.len() < MAX_CONCURRENCY_READS {
          let Some(StorageCommand::ReadPiece { meta, reply }) = read_queue.pop_front() else {
            return;
          };

          let worker = self.worker.clone();

          reads.spawn_blocking(move || {
            let result = worker.read_block(meta.piece_idx, meta.offset, meta.length);

            let message = PieceManagerMessage::StorageReply(StorageResult::ReadBlock {
              meta,
              data: result
                .as_ref()
                .map(Clone::clone)
                .map_err(|err| err.to_string()),
            });

            let _ = reply.send(message);

            result
          });
        }
      }
    });
  }
}
