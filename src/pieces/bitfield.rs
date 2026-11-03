pub fn add_piece(bitfield: &mut [u8], piece_idx: usize) {
  let byte_idx = piece_idx >> 3;

  if byte_idx >= bitfield.len() {
    return;
  }

  let bit = 1 << (7 - (piece_idx % 8));

  bitfield[byte_idx] |= bit;
}

pub fn has_piece(bitfield: &[u8], piece_idx: usize) -> bool {
  let byte_idx = piece_idx >> 3;

  if byte_idx >= bitfield.len() {
    return false;
  }
  let bit = 1 << (7 - (piece_idx % 8));

  (bitfield[byte_idx] & bit) != 0
}

pub fn count_pieces(bitfield: &[u8]) -> usize {
  let mut count = 0;

  for &byte in bitfield {
    count += byte.count_ones() as usize;
  }

  count
}
