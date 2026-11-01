use std::path::Path;

use bytes::Bytes;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::{
  event::event::TorrentEvent,
  file_parsing::metadata_types::TorrentMetadata,
  peers::peer_manager::PeerManager,
  pieces::piece_manager::PieceManager,
  scheduler::Scheduler,
  stats::Stats,
  storage_manager::StorageManager,
  trackers::{http_tracker::HttpTracker, udp_tracker::UdpTracker},
};

pub struct Torrent {
  storage_manager: StorageManager,
  piece_manager: PieceManager,
  scheduler: Scheduler,
  peer_manager: PeerManager,

  udp_tracker: UdpTracker,
  http_tracker: HttpTracker,

  stats: Stats,

  metadata: TorrentMetadata,
  info_hash: [u8; 20],
  peer_id: [u8; 20],

  pub total_bytes: u64,
  recon_receiver: UnboundedReceiver<TorrentEvent>,
}

impl Torrent {
  pub fn new(torrent_file: Bytes, download_dir: &Path) -> Self {}

  pub async fn run(self) {}

  fn get_total_bytes(&self) -> u64 {}
}
