use bytes::Bytes;
use sha1::{Digest, Sha1};

pub async fn verify_piece(piece: Bytes, expected_hash: &[u8]) -> bool {
  if expected_hash.len() != 20 {
    return false;
  }
  let mut expected = [0u8; 20];
  expected.copy_from_slice(expected_hash);
  let res = tokio::task::spawn_blocking(move || {
    let actual_hash = Sha1::digest(piece);

    actual_hash.as_slice() == expected
  });

  match res.await {
    Ok(is_valid) => is_valid,
    Err(_) => false,
  }
}
