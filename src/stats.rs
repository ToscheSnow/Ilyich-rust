use std::{collections::HashSet, time::Duration};

use serde::Serialize;
use tokio::{
  select,
  sync::{mpsc::UnboundedReceiver, watch},
  task::JoinHandle,
  time::interval,
};

use crate::{event::event::TorrentEvent, peers::peer::PeerHandle};

#[derive(Clone)]
pub struct TransferStats {
  pub downloaded: u64,
  pub uploaded: u64,
  pub left: u64,
}

#[derive(Clone, Serialize)]
pub struct StatsSnapshot {
  pub connected_peer_count: u16,
  pub choked_count: u16,
  pub seeder_count: u16,

  pub pieces_completed: u32,
  pub bytes_downloaded_this_session: u64,
  pub total_bytes_have: u64,
  pub verified_bytes: u64,
  pub uploaded_bytes: u64,
  pub last_speed_check_bytes: u64,
  pub left: u64,

  pub ema_speed: f64,

  pub total_pieces: u32,
  pub total_bytes: u64,

  pub bytes_in_progress: u64,
}

pub struct Stats {
  //peer stats
  peers: HashSet<PeerHandle>,
  choked: HashSet<PeerHandle>,
  seeders: HashSet<PeerHandle>,

  //download info
  pieces_completed: u32,
  bytes_downloaded_this_session: u64,
  total_bytes_have: u64,
  verified_bytes: u64,
  uploaded_bytes: u64,
  last_speed_check_bytes: u64,

  ema_speed: f64,

  total_pieces: u32,
  total_bytes: u64,
  recon_receiver: UnboundedReceiver<TorrentEvent>,

  stats_board_chalk: watch::Sender<StatsSnapshot>,
  stats_board: watch::Receiver<StatsSnapshot>,
}

impl Stats {
  pub fn get_stats_receiver(&self) -> watch::Receiver<StatsSnapshot> {
    self.stats_board.clone()
  }

  pub fn update_stats_board(&self) {
    let connected_peer_count = self.peers.len() as u16;
    let choked_count = self.choked.len() as u16;
    let seeder_count = self.seeders.len() as u16;

    let snap = StatsSnapshot {
      connected_peer_count,
      choked_count,
      seeder_count,
      pieces_completed: self.pieces_completed,
      bytes_downloaded_this_session: self.bytes_downloaded_this_session,
      total_bytes_have: self.total_bytes_have,
      verified_bytes: self.verified_bytes,
      uploaded_bytes: self.uploaded_bytes,
      last_speed_check_bytes: self.last_speed_check_bytes,
      ema_speed: self.ema_speed,
      total_pieces: self.total_pieces,
      total_bytes: self.total_bytes,
      bytes_in_progress: self.total_bytes_have.saturating_sub(self.verified_bytes),
      left: self.total_bytes.saturating_sub(self.verified_bytes),
    };

    let _ = self.stats_board_chalk.send(snap);
  }

  pub fn process_event(&mut self, event: TorrentEvent) {
    match event {
      TorrentEvent::PieceComplete(_, length) => {
        self.pieces_completed += 1;
        self.verified_bytes += length as u64;
      }
      TorrentEvent::Exists(_idx, len) => {
        self.pieces_completed += 1;
        self.total_bytes_have += len as u64;
        self.verified_bytes += len as u64;
      }
      TorrentEvent::BlockReceive(piece) => {
        let length = piece.block.len() as u64;
        self.bytes_downloaded_this_session += length;
        self.total_bytes_have += length;
      }

      TorrentEvent::Connect(peer) => {
        self.peers.insert(peer);
      }

      TorrentEvent::Disconnect(peer) => {
        self.peers.remove(&peer);
        self.choked.remove(&peer);
        self.seeders.remove(&peer);
      }

      TorrentEvent::BlockUpload(bytes) => {
        self.uploaded_bytes += bytes as u64;
      }
      TorrentEvent::Choke(peer) => {
        self.choked.insert(peer);
      }

      TorrentEvent::Unchoke(peer) => {
        self.choked.remove(&peer);
      }
      TorrentEvent::SeederConnect(peer) => {
        self.seeders.insert(peer);
      }
      TorrentEvent::DownloadComplete => {
        tokio::spawn(async {
          tokio::time::sleep(Duration::from_millis(2000)).await;

          println!("Download complete shutting down 😪");
          std::process::exit(0);
        });
      }
      _ => {}
    }
  }

  pub fn sample(&mut self, interval: Duration) {
    let secs = interval.as_secs_f64();
    if secs <= 0.0 {
      return;
    }

    let delta = self
      .bytes_downloaded_this_session
      .saturating_sub(self.last_speed_check_bytes);
    let inst = delta as f64 / secs;
    self.last_speed_check_bytes = self.bytes_downloaded_this_session;

    self.ema_speed = if self.ema_speed == 0.0 {
      inst
    } else {
      0.33 * inst + 0.67 * self.ema_speed
    };
  }

  pub fn new(
    total_pieces: u32,
    total_bytes: u64,
    recon_receiver: UnboundedReceiver<TorrentEvent>,
  ) -> Self {
    let (stats_board_chalk, stats_board) = tokio::sync::watch::channel(StatsSnapshot {
      connected_peer_count: 0,
      choked_count: 0,
      seeder_count: 0,
      pieces_completed: 0,
      bytes_downloaded_this_session: 0,
      total_bytes_have: 0,
      verified_bytes: 0,
      uploaded_bytes: 0,
      last_speed_check_bytes: 0,
      left: 0,
      ema_speed: 0.0,
      total_pieces,
      total_bytes,
      bytes_in_progress: 0,
    });

    Self {
      peers: HashSet::new(),
      choked: HashSet::new(),
      seeders: HashSet::new(),
      pieces_completed: 0,
      bytes_downloaded_this_session: 0,
      total_bytes_have: 0,
      verified_bytes: 0,
      uploaded_bytes: 0,
      last_speed_check_bytes: 0,
      ema_speed: 0.0,
      total_pieces,
      total_bytes,
      recon_receiver,
      stats_board_chalk,
      stats_board,
    }
  }

  // starts running
  pub fn run(mut self) -> JoinHandle<()> {
    tokio::task::spawn(async move {
      let tick_duration = Duration::from_secs(1);
      let mut ticker = interval(tick_duration);
      loop {
        select! {
            Some(event) = self.recon_receiver.recv() => {
            self.process_event(event);
          },
          _ = ticker.tick() => {
            self.sample(tick_duration);
            self.update_stats_board();
          }
        }
      }
    })
  }
}

pub fn format_bytes(n: u64) -> String {
  if n < 1024 {
    format!("{} B", n)
  } else if n < 1024_u64.pow(2) {
    format!("{:.1} KB", n as f64 / 1024.0)
  } else if n < 1024_u64.pow(3) {
    format!("{:.2} MB", n as f64 / 1024_u64.pow(2) as f64)
  } else {
    format!("{:.2} GB", n as f64 / 1024_u64.pow(3) as f64)
  }
}
