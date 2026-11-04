use bytes::Bytes;
use tokio::select;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::pieces::piece_manager::PieceManagerMessage;
use crate::pieces::verify_piece::verify_piece;

const MAX_VERIFICATION_LIMITS: u8 = 3;

pub enum ValidatorCommand {
  VerifyPiece {
    piece_idx: u32,
    data: Bytes,
    expected_hash: Bytes,
  },
}

pub struct PieceValidator {
  command_rx: mpsc::UnboundedReceiver<ValidatorCommand>,
  manager_tx: mpsc::Sender<PieceManagerMessage>,
}

impl PieceValidator {
  pub fn new(
    manager_tx: mpsc::Sender<PieceManagerMessage>,
  ) -> (Self, mpsc::UnboundedSender<ValidatorCommand>) {
    let (command_tx, command_rx) = mpsc::unbounded_channel();

    let validator = PieceValidator {
      command_rx,
      manager_tx,
    };

    (validator, command_tx)
  }

  pub fn run(mut self) {
    let mut in_progress = JoinSet::new();
    tokio::spawn(async move {
      loop {
        select! {
          Some(ValidatorCommand::VerifyPiece {piece_idx,data,expected_hash}) = self.command_rx.recv(),if in_progress.len()  < MAX_VERIFICATION_LIMITS as usize => {
            let transmitter = self.manager_tx.clone();
            let return_data = data.clone();
            in_progress.spawn_blocking(move || {
              let result = verify_piece(data,&expected_hash);

              let _ = transmitter.blocking_send(PieceManagerMessage::ValidatorResult { piece_idx, success: result,data:return_data});
            });
          },
          Some(_) = in_progress.join_next(),if !in_progress.is_empty() => {},
          else => break,
        }
      }
    });
  }
}
