use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use tokio::select;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::file_worker::FileWorker;
use crate::pieces::piece_manager::PieceManagerMessage;

const MAX_CONCURRENCY_WRITES: usize = 2;
const MAX_CONCURRENCY_READS: usize = 20;

pub struct StorageManager {
  worker: Arc<FileWorker>,
  message_rx: mpsc::UnboundedReceiver<StorageMessage>,
  manager_tx: mpsc::Sender<PieceManagerMessage>,
}

pub enum StorageCommand {
  WritePiece { piece_idx: u32, data: Bytes },
  ReadPiece(BlockMeta),
}

pub enum StorageMessage {
  Command(StorageCommand),
  Result(StorageResult),
}

#[derive(Clone)]
pub enum StorageResult {
  ReadBlock {
    meta: BlockMeta,
    data: Result<Bytes, String>,
  },
  WriteResult {
    piece_idx: u32,
    result: Result<(), Bytes>,
  },
}

#[derive(Clone)]
pub struct BlockMeta {
  pub piece_idx: u32,
  pub offset: u32,
  pub length: u32,
}

impl StorageManager {
  pub async fn new(
    mut worker: FileWorker,
    manager_tx: mpsc::Sender<PieceManagerMessage>,
  ) -> std::io::Result<(Self, mpsc::UnboundedSender<StorageMessage>)> {
    worker.init_file_handles().await;
    let (message_tx, message_rx) = mpsc::unbounded_channel::<StorageMessage>();
    let manager = StorageManager {
      worker: Arc::new(worker),
      message_rx,
      manager_tx,
    };

    Ok((manager, message_tx))
  }

  pub fn run(mut self) {
    let mut writes: JoinSet<()> = JoinSet::new();
    let mut reads: JoinSet<()> = JoinSet::new();

    let mut write_queue: VecDeque<StorageCommand> = VecDeque::new();
    let mut read_queue: VecDeque<StorageCommand> = VecDeque::new();

    tokio::spawn(async move {
      loop {
        select! {
          Some(StorageMessage::Command(cmd)) = self.message_rx.recv() =>  {
            match cmd {
                StorageCommand::WritePiece{piece_idx,data} => {write_queue.push_back(StorageCommand::WritePiece{piece_idx,data});},
                StorageCommand::ReadPiece(meta) => {read_queue.push_back(StorageCommand::ReadPiece(meta));}

            }
          },

          Some(_) = writes.join_next(), if !writes.is_empty() => {
          },

          Some(_) = reads.join_next(), if !reads.is_empty() => {},

          else => break
        }

        while writes.len() < MAX_CONCURRENCY_WRITES {
          let Some(StorageCommand::WritePiece { piece_idx, data }) = write_queue.pop_front() else {
            break;
          };

          let worker = self.worker.clone();
          let transmitter = self.manager_tx.clone();

          writes.spawn_blocking(move || {
            let data_clone = data.clone();
            let result = worker.write_piece(piece_idx, data_clone).map_err(|_| data);

            let message = PieceManagerMessage::StorageResult(StorageResult::WriteResult {
              piece_idx,
              result: result.clone(),
            });

            let _ = transmitter.blocking_send(message);
          });
        }

        while reads.len() < MAX_CONCURRENCY_READS {
          let Some(StorageCommand::ReadPiece(meta)) = read_queue.pop_front() else {
            break;
          };

          let worker = self.worker.clone();
          let transmitter = self.manager_tx.clone();

          reads.spawn_blocking(move || {
            let result = worker.read_block(meta.piece_idx, meta.offset, meta.length);

            let message = PieceManagerMessage::StorageResult(StorageResult::ReadBlock {
              meta,
              data: result
                .as_ref()
                .map(Clone::clone)
                .map_err(|err| err.to_string()),
            });

            let _ = transmitter.blocking_send(message);
          });
        }
      }
    });
  }
}
