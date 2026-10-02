use std::{
  collections::HashMap,
  io::{ErrorKind::NotFound, SeekFrom},
  path::PathBuf,
};

use bytes::{Bytes, BytesMut};
use tokio::{
  fs::{File, OpenOptions},
  io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

pub struct DiskFile {
  start_offset: u64,
  length: u64,
  path: PathBuf,
}

pub struct StorageManager {
  files: Vec<DiskFile>,
  piece_length: u32,
  file_handles: HashMap<PathBuf, File>,
  meta: TorrentMetadata,
  dir_path: PathBuf,
}

pub struct TorrentMetadata {
  info_hash: [u8; 20],
  name: String,
  piece_length: u32,
  piece_hashes: Vec<[u8; 20]>,
  private: Option<u8>,
  layout: FileLayout,
}

pub struct TorrentFile {
  length: u64,
  path: Vec<String>,
}

enum FileLayout {
  SingleFileLayout { length: u64 },

  MultiFileInfo { files: Vec<TorrentFile> },
}

impl StorageManager {
  pub fn new(meta: TorrentMetadata, dir_path: PathBuf) -> Self {
    let piece_length = meta.piece_length;

    let mut manager = Self {
      piece_length,
      file_handles: HashMap::new(),
      dir_path,
      files: Vec::new(),
      meta,
    };

    manager.init_file_paths();

    return manager;
  }

  fn init_file_paths(&mut self) {
    match &self.meta.layout {
      FileLayout::SingleFileLayout { length } => {
        self.files.push(DiskFile {
          start_offset: 0,
          length: *length,

          // join directory path with file_path
          path: self.dir_path.join(PathBuf::from(&self.meta.name)),
        });
      }

      FileLayout::MultiFileInfo { files } => {
        //last_end == curr_start
        let mut last_end = 0;

        // borrow curr from files
        for curr in files {
          let mut path = self.dir_path.clone();

          for sub in &curr.path {
            path.push(sub);
          }

          //Make a DiskFile and store into files
          self.files.push(DiskFile {
            start_offset: last_end,
            length: curr.length,
            path,
          });

          last_end += curr.length;
        }
      }
    }
  }

  pub async fn init_file_handles(&mut self) -> std::io::Result<()> {
    for DiskFile { length, path, .. } in &self.files {
      let Some(parent_dir) = path.parent() else {
        return Err(std::io::Error::new(
          std::io::ErrorKind::InvalidData,
          "Parent directory could not be resolved",
        ));
      };

      tokio::fs::create_dir_all(parent_dir).await?;

      let file_handle = match OpenOptions::new().read(true).write(true).open(path).await {
        // file exists
        Ok(file) => file,

        //Error No Entry create the file and set its length
        Err(err) if err.kind().eq(&NotFound) => {
          let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .await?;

          file.set_len(*length).await?;
          file
        }

        //Some other error
        Err(err) => return Err(err),
      };

      self.file_handles.insert(path.clone(), file_handle);
    }

    Ok(())
  }

  pub async fn write_piece(
    &mut self,
    piece_idx: u32,
    piece_buf: Bytes,
  ) -> Result<(), std::io::Error> {
    let piece_offset: u64 = self.piece_length as u64 * piece_idx as u64;
    let piece_end: u64 = piece_offset + piece_buf.len() as u64;
    for DiskFile {
      start_offset,
      length,
      path: file_path,
    } in &self.files
    {
      if *start_offset + *length <= piece_offset.into() {
        continue;
      };

      if piece_end <= *start_offset {
        continue;
      };

      let file_handle = self
        .file_handles
        .get_mut(file_path)
        .expect("Files are opened before io");

      let (start, end) = interval_intersection(
        piece_offset,
        piece_end,
        *start_offset,
        *start_offset + *length,
      );

      let buffer_offset = (start - piece_offset) as usize;
      let file_offset = start - start_offset;
      let bytes_to_write = (end - start) as usize;

      // file_handle.write
      file_handle.seek(SeekFrom::Start(file_offset)).await?;
      file_handle
        .write_all(&piece_buf[buffer_offset..buffer_offset + bytes_to_write])
        .await?;

      file_handle.flush().await?;
    }
    return Ok(());
  }

  pub async fn read_block(
    &mut self,
    piece_idx: u32,
    offset: u32,
    len: u32,
  ) -> Result<Bytes, std::io::Error> {
    // piece
    // 100 250

    //file x 20 100
    // file a 80 150
    //file b 150 200
    // file c 200 250
    // file d 250 900
    let mut buf = BytesMut::zeroed(len as usize);

    let piece_offset = self.piece_length as u64 * piece_idx as u64 + offset as u64;

    let piece_end = piece_offset + len as u64;

    for DiskFile {
      start_offset: file_offset,
      length: file_length,
      path: file_path,
    } in &self.files
    {
      if piece_offset as u64 >= file_offset + file_length {
        continue;
      };

      if piece_end <= *file_offset {
        continue;
      };

      let (start, end) = interval_intersection(
        piece_offset,
        piece_end,
        *file_offset,
        file_offset + file_length,
      );

      let buf_offset = (start - piece_offset as u64) as usize;
      let buf_end = (end - piece_offset as u64) as usize;

      let file_handle = self
        .file_handles
        .get_mut(file_path)
        .expect("Files are opened before io");

      file_handle
        .seek(SeekFrom::Start(start - file_offset))
        .await?;

      file_handle
        .read_exact(&mut buf[buf_offset..buf_end])
        .await?;
    }

    return Ok(buf.freeze());
  }
}

pub fn interval_intersection(
  piece_start: u64,
  piece_end: u64,
  file_start: u64,
  file_end: u64,
) -> (u64, u64) {
  return (file_start.max(piece_start), file_end.min(piece_end));
}
