use std::{collections::HashMap, io::ErrorKind::NotFound, path::PathBuf, sync::Arc};

use bytes::{Bytes, BytesMut};
use std::fs::OpenOptions;

pub struct DiskFile {
  start_offset: u64,
  length: u64,
  path: PathBuf,
}

pub struct FileWorker {
  files: Vec<DiskFile>,
  piece_length: u32,
  file_handles: HashMap<PathBuf, Arc<std::fs::File>>,
  meta: TorrentMetadata,
  dir_path: PathBuf,
}

#[derive(Clone)]
pub struct TorrentMetadata {
  pub info_hash: [u8; 20],
  pub name: String,
  pub piece_length: u32,
  pub piece_hashes: Bytes,
  pub private: Option<u8>,
  pub layout: FileLayout,
}

#[derive(Debug, Clone)]
pub struct TorrentFile {
  pub length: u64,
  pub path: Vec<String>,
}

#[derive(Clone)]
pub enum FileLayout {
  SingleFileLayout { length: u64 },
  MultiFileLayout { files: Vec<TorrentFile> },
}

impl FileWorker {
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

    manager
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

      FileLayout::MultiFileLayout { files } => {
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

      let file_handle = match OpenOptions::new().read(true).write(true).open(path) {
        // file exists
        Ok(file) => file,

        //Error No Entry create the file and set its length
        Err(err) if err.kind().eq(&NotFound) => {
          let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

          file.set_len(*length)?;
          file
        }

        //Some other error
        Err(err) => return Err(err),
      };

      self
        .file_handles
        .insert(path.clone(), Arc::new(file_handle));
    }

    Ok(())
  }

  pub fn write_piece(&self, piece_idx: u32, piece_buf: Bytes) -> Result<(), std::io::Error> {
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
        .get(file_path)
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
      write_all(
        file_handle,
        &piece_buf[buffer_offset..buffer_offset + bytes_to_write],
        file_offset,
      )?;
    }
    return Ok(());
  }

  pub fn read_block(&self, piece_idx: u32, offset: u32, len: u32) -> Result<Bytes, std::io::Error> {
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
        .get(file_path)
        .expect("Files are opened before io");

      read_exact(
        file_handle,
        &mut buf[buf_offset..buf_end],
        start - *file_offset,
      )?;
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

#[cfg(unix)]
fn write_at(file: &std::fs::File, buf: &[u8], offset: u64) -> std::io::Result<usize> {
  use std::os::unix::fs::FileExt;
  file.write_at(buf, offset)
}

#[cfg(windows)]
fn write_at(file: &std::fs::File, buf: &[u8], offset: u64) -> std::io::Result<usize> {
  use std::os::windows::fs::FileExt;
  file.seek_write(buf, offset)
}

#[cfg(unix)]
fn read_at(file: &std::fs::File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
  use std::os::unix::fs::FileExt;
  file.read_at(buf, offset)
}

#[cfg(windows)]
fn read_at(file: &std::fs::File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
  use std::os::windows::fs::FileExt;
  file.seek_read(buf, offset)
}

fn write_all(file: &std::fs::File, mut data: &[u8], mut offset: u64) -> std::io::Result<()> {
  while !data.is_empty() {
    let written_bytes = write_at(file, data, offset)?;

    if written_bytes == 0 {
      return Err(std::io::ErrorKind::WriteZero.into());
    }

    offset += written_bytes as u64;
    data = &data[written_bytes..];
  }

  Ok(())
}

fn read_exact(file: &std::fs::File, mut buf: &mut [u8], mut offset: u64) -> std::io::Result<()> {
  while !buf.is_empty() {
    let read = read_at(file, buf, offset)?;

    if read == 0 {
      return Err(std::io::ErrorKind::UnexpectedEof.into());
    }

    offset += read as u64;
    buf = &mut buf[read..];
  }

  Ok(())
}
