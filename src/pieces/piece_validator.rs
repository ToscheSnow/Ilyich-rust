use bytes::Bytes;
use tokio::select;
use tokio::sync::mpsc;
use tokio::{sync::oneshot, task::JoinSet};

use crate::pieces::piece_manager::PieceManagerMessage;
use crate::pieces::verify_piece::verify_piece;

pub enum ValidatorCommand {
  VerifyPiece {
    piece_idx: u32,
    data: Bytes,
    expected_hash: Bytes,
    reply: oneshot::Sender<PieceManagerMessage>,
  },
}

const MAX_VERIFICATION_LIMITS: u8 = 4;

pub struct PieceValidator {
  command_rx: mpsc::Receiver<ValidatorCommand>,
}

impl PieceValidator {
  pub fn new() -> (Self, mpsc::Sender<ValidatorCommand>) {
    let (command_tx, command_rx) = mpsc::channel(10);

    let validator = PieceValidator { command_rx };

    (validator, command_tx)
  }

  pub fn run(mut self) {
    let mut in_progress = JoinSet::new();
    tokio::spawn(async move {
      loop {
        select! {
          Some(ValidatorCommand::VerifyPiece {piece_idx,data,expected_hash,reply}) = self.command_rx.recv(),if in_progress.len()  < MAX_VERIFICATION_LIMITS as usize => {
            in_progress.spawn_blocking(move || {
              let result = verify_piece(data,&expected_hash);

              let _ = reply.send(PieceManagerMessage::ValidatorReply {piece_idx,success:result});
            });
          },
          Some(_) = in_progress.join_next(),if !in_progress.is_empty() => {},
          else => break,
        }
      }
    });
  }
}
