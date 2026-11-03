mod StorageManager;
mod event;
mod file_parsing;
mod file_worker;
mod peer_protocol;
mod peers;
mod pieces;
mod recon;
mod scheduler;
mod stats;
mod torrent;
mod trackers;
mod ui;
mod utils;

use std::{env, path::Path};

use crate::utils::open_file;

#[tokio::main]
async fn main() {
  let args: Vec<String> = env::args().collect();

  if args.len() < 3 {
    println!("Please provide torrent file path and download location");
    return;
  }

  let torrent_path = &args[1];
  let download_location = &args[2];

  let file = open_file::open_file(Path::new(torrent_path));
  let torrent = torrent::Torrent::new(file, Path::new(download_location));

  torrent.run().await;
}
