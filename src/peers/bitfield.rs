use std::collections::HashSet;

use bytes::{Bytes, BytesMut};

pub fn add_pieces_from_bitfield(received_bitfield: Bytes, peer_pieces: &mut HashSet<u32>) -> () {
  let mut counter = 0;

  for byte in received_bitfield {
    for i in 0..8 {
      if (byte >> (7 - i)) & 1 == 1 {
        peer_pieces.insert(counter);
        counter += 1;
      }
    }
  }
}

pub fn BITFIELD_BUF(verified_pieces: &HashSet<u32>, piece_count: u32) -> Bytes {
  let mut bitfield = BytesMut::zeroed((piece_count as usize).div_ceil(8));

  for piece_idx in verified_pieces {
    let byte_idx = piece_idx.div_euclid(8);
    let bit_idx = piece_idx % 8;

    bitfield[byte_idx as usize] |= 1 << (7 - bit_idx);
  }

  return bitfield.freeze();
}
