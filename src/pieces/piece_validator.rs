const BLOCK_SIZE: u32 = 16384;

pub struct PieceValidator {
  total_length: u32,
  piece_length: u32,
}

impl PieceValidator {
  pub fn new(total_length: u32, piece_length: u32) -> Self {
    Self {
      total_length,
      piece_length,
    }
  }

  pub fn piece_count(&self) -> u32 {
    if self.piece_length == 0 {
      return 0;
    }
    self.total_length.div_ceil(self.piece_length)
  }

  pub fn get_piece_length(&self, piece_idx: i32) -> u32 {
    if piece_idx < 0 || piece_idx as u32 >= self.piece_count() {
      panic!("Invalid piece index");
    }

    if piece_idx as u32 == self.piece_count() - 1 {
      return self.total_length - (piece_idx as u32 * self.piece_length);
    }

    self.piece_length
  }

  pub fn get_block_count(&self, piece_idx: i32) -> u32 {
    let length = self.get_piece_length(piece_idx);

    if BLOCK_SIZE == 0 {
      return 0;
    }
    length.div_ceil(BLOCK_SIZE)
  }

  pub fn get_block_length(&self, piece_idx: i32, offset: i32) -> u32 {
    let piece_length = self.get_piece_length(piece_idx);

    if offset < 0 || offset as u32 >= piece_length {
      panic!("Invalid block offset");
    }

    std::cmp::min(BLOCK_SIZE, piece_length - offset as u32)
  }

  pub fn is_valid_block(&self, piece_idx: i32, offset: i32, length: u32) -> bool {
    if piece_idx < 0 || piece_idx as u32 >= self.piece_count() {
      return false;
    }

    if offset < 0 || offset as u32 >= self.get_piece_length(piece_idx) {
      return false;
    }

    length == self.get_block_length(piece_idx, offset)
  }
}
