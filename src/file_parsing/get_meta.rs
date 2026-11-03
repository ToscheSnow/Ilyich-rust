use crate::file_parsing::info_hash::get_info_hash;
use crate::file_parsing::parser_types::{BencodeDict, BencodeVal};
use crate::file_worker::{FileLayout, TorrentFile, TorrentMetadata};

pub fn get_meta(decoded_torrent: &BencodeDict) -> Result<TorrentMetadata, String> {
  let info = match decoded_torrent.get("info") {
    Some(BencodeVal::Dict(info)) => info,
    _ => return Err("Missing or invalid info dictionary".into()),
  };

  let info_hash = get_info_hash(decoded_torrent)?;

  let name = match info.get("name") {
    Some(BencodeVal::Bytes(bytes)) => String::from_utf8_lossy(bytes).into_owned(),
    _ => return Err("Missing or invalid torrent name".into()),
  };

  let piece_length = match info.get("piece length") {
    Some(BencodeVal::Int(value)) => *value,
    _ => return Err("Missing or invalid piece length".into()),
  };

  let piece_hashes = match info.get("pieces") {
    Some(BencodeVal::Bytes(bytes)) => bytes.clone(),
    _ => return Err("Missing or invalid piece hashes".into()),
  };

  // Single file
  if let Some(BencodeVal::Int(length)) = info.get("length") {
    return Ok(TorrentMetadata {
      name,
      piece_length: piece_length as u32,
      piece_hashes,
      layout: FileLayout::SingleFileLayout {
        length: *length as u64,
      },
      private: None,
      info_hash,
    });
  }

  // Multi-file
  let raw_files = match info.get("files") {
    Some(BencodeVal::List(files)) => files,
    _ => return Err("Missing or invalid files list".into()),
  };

  let mut files: Vec<TorrentFile> = Vec::new();

  for file in raw_files {
    let file_dict = match file {
      BencodeVal::Dict(dict) => dict,
      _ => return Err("Invalid file entry".into()),
    };

    let length = match file_dict.get("length") {
      Some(BencodeVal::Int(value)) => *value,
      _ => return Err("Missing or invalid file length".into()),
    } as u64;

    let raw_path = match file_dict.get("path") {
      Some(BencodeVal::List(path)) => path,
      _ => return Err("Missing or invalid file path".into()),
    };

    let mut path: Vec<String> = Vec::new();

    for component in raw_path {
      match component {
        BencodeVal::Bytes(bytes) => {
          path.push(String::from_utf8_lossy(bytes).into_owned());
        }
        _ => return Err("Invalid path component".into()),
      }
    }

    files.push(TorrentFile { length, path });
  }

  Ok(TorrentMetadata {
    info_hash: info_hash.into(),
    name,
    piece_length: piece_length as u32,
    piece_hashes,
    layout: FileLayout::MultiFileLayout { files },
    private: None,
  })
}
