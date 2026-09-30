use std::time::Instant;

pub enum PeerMessage {
  CHOKE,
  UNCHOKE,
  INTERESTED,
  NOT_INTERESTED,
  HAVE {
    piece_id: u32,
  },

  BITFIELD {
    bitfield: Vec<u8>,
  },

  REQUEST(BlockRequest),

  PIECE {
    piece_id: u32,
    offset: u32,
    data: Vec<u8>,
  },

  CANCEL(BlockRequest),

  PORT,
  EXTENDED,
}

pub struct BlockRequest {
  pub piece_id: u32,
  pub offset: u32,
  pub length: u32,
}

pub struct PendingReq {
  pub piece_id: u32,
  pub offset: u32,
  pub length: u32,

  pub sent_at: Instant,
}

pub enum ManagerMessage {
  PieceReceived {
    piece_id: u32,
    offset: u32,
    data: Vec<u8>,
  },
}
