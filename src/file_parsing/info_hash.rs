use crate::file_parsing::encoder::BencodeEncoder;
use crate::file_parsing::parser_types::BencodeDict;
use sha1::{Digest, Sha1};

pub fn get_info_hash(decoded_torrent: &BencodeDict) -> Result<[u8;20], String> {
  // Encoder to encode info dict
  let encoder = BencodeEncoder::new();

  // Encode only the torrent info portion using BencodeEncoder
  let info = decoded_torrent
    .get("info")
    .ok_or("Missing info dictionary")?;

  let encoded = encoder.encode(info);

  let info_hash = Sha1::digest(&encoded);

  // Return as a byte vector, process at call site
  Ok(info_hash.into())
}
