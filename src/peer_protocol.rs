use bytes::Bytes;
use std::time::Instant;

pub enum PeerEvent {
  CHOKE,
  UNCHOKE,
  INTERESTED,
  NotInterested,
  HAVE {
    piece_idx: u32,
  },

  BITFIELD {
    bitfield: Bytes,
  },

  REQUEST(BlockRequest),

  PIECE {
    piece_idx: u32,
    offset: u32,
    block: Bytes,
  },

  CANCEL(BlockRequest),

  PORT(u16),
  EXTENDED(u8),
}

#[derive(Clone)]
pub struct BlockRequest {
  pub piece_idx: u32,
  pub offset: u32,
  pub length: u32,
}

pub struct PendingReq {
  pub piece_idx: u32,
  pub offset: u32,
  pub length: u32,

  pub sent_at: Instant,
}
