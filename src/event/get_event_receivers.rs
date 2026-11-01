use tokio::sync::mpsc::UnboundedReceiver;

use crate::{
  event::event::{Topic, TorrentEvent}, recon::{Recon, ReconBuilder},
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
  ]);

  let stats = b.subscribe([
    Topic::BlockReceive,
    Topic::BlockUpload,
    Topic::PieceComplete,
    Topic::DownloadComplete,
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
