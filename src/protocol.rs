pub enum PeerMessage {
  CHOKE,
  UNCHOKE,
  INTERESTED,
  NOT_INTERESTED,
  HAVE {
    pieceId: usize,
  },

  BITFIELD {
    bitfield: Vec<u8>,
  },

  REQUEST(BlockRequest),

  PIECE {
    pieceIdx: usize,
    offset: usize,
    data: Vec<u8>,
  },

  CANCEL(BlockRequest),

  PORT,
  EXTENDED,
}

pub struct BlockRequest {
  pub pieceIdx: usize,
  pub offset: usize,
  pub length: usize,
}

pub struct PendingReq {
  pub pieceIdx: usize,
  pub offset: usize,
  pub length: usize,

  pub sentAt: usize,
}

pub enum ManagerMessage {
  PieceReceived {
    pieceIdx: usize,
    offset: usize,
    data: Vec<u8>,
  },
}
