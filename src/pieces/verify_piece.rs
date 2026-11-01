use bytes::Bytes;
use sha1::{Digest, Sha1};

pub async fn verify_piece(piece: Bytes, expected_hash: &[u8; 20]) -> bool {
  let expected = expected_hash.clone();
  let res = tokio::task::spawn_blocking(move || {
    let actual_hash = Sha1::digest(piece);

    actual_hash.as_slice() == expected
  });

  match res.await {
    Ok(is_valid) => is_valid,
    Err(_) => false,
  }
}
