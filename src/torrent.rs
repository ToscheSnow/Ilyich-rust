use std::{
  collections::HashSet,
  path::{Path, PathBuf},
  sync::{Arc, RwLock},
};

use bytes::Bytes;
use tokio::sync::mpsc::{self, Sender};

use crate::{
  event::get_event_receivers::{self, Receivers},
  file_parsing::{get_meta::get_meta, parser_types::BencodeVal, parserDecoder::BencodeDecoder},
  file_worker::{
    FileLayout::{MultiFileLayout, SingleFileLayout},
    FileWorker, TorrentMetadata,
  },
  peers::{
    client::CLIENT_ID_BYTES,
    peer_manager::{PeerManager, PeerManagerMessage},
  },
  pieces::{
    piece_manager::{PieceManager, PieceManagerMessage},
    piece_validator::PieceValidator,
  },
  scheduler::Scheduler,
  stats::Stats,
  storage_manager::StorageManager,
  trackers::{
    extract_url::extract_tracker_urls,
    http_tracker::{HttpTracker, TrackerAnnounceArgs},
    udp_tracker::UdpTracker,
  },
  ui::start_stats_ui,
};

pub struct Torrent {
  piece_manager: PieceManager,
  scheduler: Scheduler,
  peer_manager: PeerManager,
  udp_tracker: UdpTracker,
  http_tracker: HttpTracker,
  stats: Stats,
}

impl Torrent {
  pub async fn new(torrent_file: Bytes, download_dir: &Path) -> std::io::Result<Self> {
    let mut decoder = BencodeDecoder::new(torrent_file.to_vec());
    let Ok(BencodeVal::Dict(decoded_torrent)) = decoder.parse() else {
      return Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "Couldn't parse the torrent file",
      ));
    };

    let meta = get_meta(&decoded_torrent).unwrap();
    let info_hash = meta.info_hash;
    let total_length = match &meta.layout {
      SingleFileLayout { length } => *length,
      MultiFileLayout { files } => files.iter().map(|file| file.length).sum(),
    };

    let total_pieces: u32 = total_length.div_ceil(meta.piece_length as u64) as u32;

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

    // the piece manager's channel has to exist BEFORE the workers,
    // because storage and validator need a sender to report back 📬
    let (piece_manager_sender, piece_manager_receiver) =
      mpsc::channel::<PieceManagerMessage>(200000);

    let storage_manager = FileWorker::new(meta_clone, PathBuf::from(download_dir));
    let (storage, storage_tx) =
      StorageManager::new(storage_manager, piece_manager_sender.clone()).await?;
    storage.run();

    let (validator, validator_tx) = PieceValidator::new(piece_manager_sender.clone());
    validator.run();

    let piece_manager = PieceManager::new(
      meta.clone(),
      storage_tx,
      validator_tx,
      recon.clone(),
      piece_manager_receiver,
    );

    let active_peers = Arc::new(RwLock::new(HashSet::new()));

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

    let http_tracker = HttpTracker::new(
      http_urls,
      TrackerAnnounceArgs {
        info_hash,
        peer_id: *CLIENT_ID_BYTES,
        port: 9090,
      },
      stats_board.clone(),
      peer_manager_sender.clone(),
      total_length,
    );

    let udp_tracker = UdpTracker::new(
      udp_urls,
      tracker_args,
      stats_board,
      peer_manager_sender.clone(),
      total_length,
    );

    Ok(Torrent {
      piece_manager,
      peer_manager,
      stats,
      udp_tracker,
      scheduler,
      http_tracker,
    })
  }

  pub async fn run(self) {
    let ui = start_stats_ui(self.stats.get_stats_receiver());
    self.stats.run();

    self.piece_manager.run().await;

    self.peer_manager.run();
    self.scheduler.run();
    self.udp_tracker.run();
    self.http_tracker.run();

    let _ = ui.await;
  }
}
