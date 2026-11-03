use ratatui::widgets::LegendPosition::Top;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::{
  event::event::{Topic, TorrentEvent},
  recon::{Recon, ReconBuilder},
  stats::TransferStats,
};

pub struct Receivers {
  pub pieces: UnboundedReceiver<TorrentEvent>,
  pub peers: UnboundedReceiver<TorrentEvent>,
  pub scheduler: UnboundedReceiver<TorrentEvent>,
  pub stats: UnboundedReceiver<TorrentEvent>,
}

/// Adjust the topic lists to whatever each component actually needs.
pub fn build() -> (Recon<TorrentEvent>, Receivers) {
  let mut b = ReconBuilder::<TorrentEvent>::new();

  let pieces = b.subscribe([
    Topic::BlockReceive,
    Topic::PieceComplete,
    Topic::PieceFail,
    Topic::Have,
    Topic::Bitfield,
    Topic::Exists,
    Topic::DownloadComplete,
  ]);

  let peers = b.subscribe([
    Topic::Connect,
    Topic::SeederConnect,
    Topic::Disconnect,
    Topic::Choke,
    Topic::Unchoke,
    Topic::Ban,
    Topic::IncomingRequest,
    Topic::DownloadComplete,
  ]);

  let scheduler = b.subscribe([
    Topic::Have,
    Topic::Bitfield,
    Topic::Unchoke,
    Topic::Disconnect,
    Topic::PieceFail,
    Topic::PieceComplete,
    Topic::DownloadComplete,
    Topic::Choke,
    Topic::Unchoke,
    Topic::Exists,
  ]);

  let stats = b.subscribe([
    Topic::BlockReceive,
    Topic::BlockUpload,
    Topic::PieceComplete,
    Topic::DownloadComplete,
    Topic::Connect,
    Topic::Disconnect,
    Topic::Choke,
    Topic::SeederConnect,
    Topic::Exists,
    Topic::Unchoke,
  ]);

  let recon = b.build();

  (
    recon,
    Receivers {
      pieces,
      peers,
      scheduler,
      stats,
    },
  )
}
