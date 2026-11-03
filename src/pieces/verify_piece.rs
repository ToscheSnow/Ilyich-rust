use bytes::Bytes;
use sha1::{Digest, Sha1};

pub fn verify_piece(piece: Bytes, expected_hash: &[u8]) -> bool {
  if expected_hash.len() != 20 {
    return false;
  }
  let mut expected = [0u8; 20];
  expected.copy_from_slice(expected_hash);
  let actual_hash = Sha1::digest(piece);

  actual_hash.as_slice() == expected
}
