use bytes::Bytes;
use std::io::ErrorKind;
use std::path::Path;

pub fn open_file(file_path: &Path) -> Bytes {
  match std::fs::read(file_path) {
    Ok(read_bytes) => Bytes::from(read_bytes),

    Err(err) if err.kind() == ErrorKind::NotFound => {
      panic!("The input torrent file doesnt exist at the given path 😰");
    }

    Err(err) => {
      panic!("An unexpected file error occurred: {err}");
    }
  }
}
