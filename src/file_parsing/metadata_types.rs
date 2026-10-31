#[derive(Debug, Clone)]
pub struct TorrentFile {
  pub length: i64,
  pub path: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum TorrentMetadata {
  SingleFile {
    name: String,
    piece_length: i64,
    piece_hashes: Vec<u8>,
    length: i64,
    info_hash: Vec<u8>,
  },
  MultiFile {
    name: String,
    piece_length: i64,
    piece_hashes: Vec<u8>,
    info_hash: Vec<u8>,
    files: Vec<TorrentFile>,
  },
}
