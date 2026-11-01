use std::{collections::HashSet};

use tokio::{
  select, sync::{self, mpsc::UnboundedReceiver, watch},
};

use crate::{event::TorrentEvent, peers::peer::PeerHandle};

pub struct TransferStats {
  downloaded: u64,
  uploaded: u64,
  left: u64,
}

pub struct StatsSnapshot {
  connected_peer_count: u16,
  choked_count: u16,
  seeder_count: u16,

  pieces_completed: u32,
  bytes_downloaded_this_session: u64,
  total_bytes_have: u64,
  verified_bytes: u64,
  uploaded_bytes: u64,
  last_speed_check_bytes: u64,
  left: u64,

  ema_speed: f64,

  total_pieces: u32,
  total_bytes: u64,

  bytes_in_progress: u64,
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

  pub sup

  pub fn update_stats_board(&self) {
    // Safely cast lengths to u16 preventing silent overflow wrap-around
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
      bytes_in_progress: self.total_bytes_have - self.verified_bytes,
      left: (self.total_bytes - self.total_bytes_have).max(0),
    };

    let _ = self.stats_board_chalk.send(snap);
  }

  pub fn process_event(&mut self, event: TorrentEvent) {
    match event {
      TorrentEvent::PieceComplete(_, length) => {
        self.pieces_completed += 1;
        self.verified_bytes += length as u64;
      }
      TorrentEvent::Exists(len) => {
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
      _ => {}
    }
  }

  pub fn sample(&mut self,interval:Duration) {
    let secs = interval.as_secs_f64();
    if secs <= 0.0 { return; }
  
    let delta = self.bytes_downloaded_this_session - self.last_speed_check_bytes;
    let inst = delta as f64 / secs;
    self.last_speed_check_bytes = self.bytes_downloaded_this_session;
  
    self.ema_speed = if self.ema_speed == 0.0 {
      inst
    } else {
      0.33 * inst + 0.67 * self.ema_speed
    };
  }

  pub fn new(total_pieces:u32,total_bytes:u64,recon_receiver:UnboundedReceiver<TorrentEvent>) -> Self {
    let (stats_board_chalk,stats_board) = tokio::sync::watch::channel(TorrentEvent);
    Self { peers: HashSet::new(), choked: HashSet::new(), seeders: HashSet::new(), pieces_completed: 0, bytes_downloaded_this_session: 0, total_bytes_have: 0, verified_bytes: 0, uploaded_bytes: 0, last_speed_check_bytes: 0, ema_speed: 0.0, total_pieces, total_bytes, recon_receiver, stats_board_chalk, stats_board }
  }

  // starts running 
  pub fn run(mut self) -> JoinHandle<()> {
    tokio::task::spawn(async { loop {} })
  }
}
