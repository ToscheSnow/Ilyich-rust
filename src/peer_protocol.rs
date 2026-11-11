use bytes::Bytes;
use std::time::Instant;

use crate::storage_manager::BlockMeta;

pub enum PeerEvent {
  CHOKE,
  UNCHOKE,
  INTERESTED,
  NotInterested,

  HAVE(u32),
  BITFIELD(Bytes),

  REQUEST(BlockMeta),

  PIECE {
    piece_idx: u32,
    offset: u32,
    block: Bytes,
  },

  CANCEL(BlockMeta),

  PORT(u16),
  EXTENDED(u8),
}

pub struct PendingReq {
  pub piece_idx: u32,
  pub offset: u32,
  pub length: u32,

  pub sent_at: Instant,
}
