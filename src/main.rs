mod event;
mod file_parsing;
mod peer_protocol;
mod peers;
mod pieces;
mod recon;
// mod scheduler;
mod stats;
mod storage_manager;
mod torrent;
// mod trackers;
mod utils;

use std::path::Path;

use crate::utils::open_file;

#[tokio::main]
async fn main() {
  let file = open_file::open_file(Path::new("./sp.torrent"));

  let torrent = torrent::Torrent::new(file, Path::new("./download"));

  torrent.run().await;
}
