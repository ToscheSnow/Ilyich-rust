use std::{
  collections::HashSet,
  net::SocketAddr,
  path::{Path, PathBuf},
  sync::{Arc, RwLock},
};

use bytes::Bytes;
use tokio::{select, sync::mpsc::Sender};

use crate::{
  event::get_event_receivers::{self, Receivers},
  file_parsing::{get_meta::get_meta, parser_types::BencodeVal, parserDecoder::BencodeDecoder},
  peers::{
    client::CLIENT_ID_BYTES,
    peer_manager::{PeerManager, PeerManagerMessage},
  },
  pieces::piece_manager::PieceManager,
  scheduler::Scheduler,
  stats::Stats,
  file_worker::{
    FileLayout::{MultiFileLayout, SingleFileLayout},
    FileWorker, TorrentMetadata,
  },
  trackers::{
    self, extract_url::extract_tracker_urls, http_tracker::TrackerAnnounceArgs,
    udp_tracker::UdpTracker,
  },
  ui::start_stats_ui,
};

pub struct Torrent {
  piece_manager: PieceManager,
  scheduler: Scheduler,
  peer_manager: PeerManager,

  udp_tracker: UdpTracker,
  // http_tracker: HttpTracker,
  stats: Stats,
  meta: TorrentMetadata,
  info_hash: [u8; 20],
  peer_id: [u8; 20],

  pub total_bytes: u64,

  peer_manager_sender: Sender<PeerManagerMessage>, // recon_receiver: UnboundedReceiver<TorrentEvent>,
}

impl Torrent {
  pub fn new(torrent_file: Bytes, download_dir: &Path) -> Self {
    let mut decoder = BencodeDecoder::new(torrent_file.to_vec());

    let Ok(BencodeVal::Dict(decoded_torrent)) = decoder.parse() else {
      panic!("Couldnt parse the torrent_file");
    };

    let meta = get_meta(&decoded_torrent).unwrap();
    let info_hash = meta.info_hash;
    let total_length = match &meta.layout {
      // <-- Add the ampersand (&) here
      SingleFileLayout { length } => *length,
      MultiFileLayout { files } => files.iter().map(|file| file.length).sum(),
    };

    let total_pieces: u32 = total_length.div_ceil(meta.piece_length as u64) as u32;

    // let total_bytes=;
    let (
      recon,
      Receivers {
        pieces,
        peers,
        scheduler,
        stats,
      },
    ) = get_event_receivers::build();

    let recon_clone = recon.clone();
    let meta_clone = meta.clone();

    let storage_manager = FileWorker::new(meta_clone, PathBuf::from(download_dir));
    let active_peers = Arc::new(RwLock::new(HashSet::new()));

    let (piece_manager, piece_manager_sender) =
      PieceManager::new(meta.clone(), storage_manager, recon);

    let (peer_manager, peer_manager_sender) = PeerManager::new(
      active_peers.clone(),
      Bytes::copy_from_slice(&meta.info_hash),
      recon_clone,
      peers,
      piece_manager_sender,
      total_pieces as u32,
    );

    let stats = Stats::new(total_pieces, total_length, stats);
    let stats_board = stats.get_stats_receiver();

    let (http_urls, udp_urls) = extract_tracker_urls(&decoded_torrent);

    let scheduler = Scheduler::new(active_peers, scheduler, total_length, meta.piece_length);

    let tracker_args = TrackerAnnounceArgs {
      info_hash,
      peer_id: *CLIENT_ID_BYTES,
      port: 9090,
    };

    let udp_tracker = UdpTracker::new(
      udp_urls,
      tracker_args,
      stats_board,
      peer_manager_sender.clone(),
      total_length,
    );

    let torrent = Torrent {
      info_hash,
      meta,
      piece_manager,
      peer_manager,
      peer_id: *CLIENT_ID_BYTES,
      total_bytes: total_length,
      peer_manager_sender,
      stats,
      udp_tracker,
      scheduler,
    };
    torrent
  }

  pub async fn run(self) {
    let ui = start_stats_ui(self.stats.get_stats_receiver());
    self.stats.run();
    self.piece_manager.run().await;
    self.peer_manager.run();
    self.scheduler.run();
    self.udp_tracker.run();

    let _ = ui.await;
    // let peers = Vec::new();
  }

  // fn get_total_bytes(&self) -> u64 {}
}
