use tokio::sync::mpsc::UnboundedReceiver;

use crate::{
  event::event::{Topic, TorrentEvent},
  recon::{Recon, ReconBuilder},
};

pub struct Receivers {
  pub peers: UnboundedReceiver<TorrentEvent>,
  pub scheduler: UnboundedReceiver<TorrentEvent>,
  pub stats: UnboundedReceiver<TorrentEvent>,
}

/// Adjust the topic lists to whatever each component actually needs.
pub fn build() -> (Recon<TorrentEvent>, Receivers) {
  let mut b = ReconBuilder::<TorrentEvent>::new();

  let peers = b.subscribe([
    Topic::Connect,
    Topic::SeederConnect,
    Topic::Disconnect,
    Topic::Choke,
    Topic::Unchoke,
    Topic::Ban,
    Topic::IncomingRequest,
    Topic::DownloadComplete,
    Topic::PieceComplete,
    Topic::Exists,
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
    Topic::BlockReceive,
    Topic::Connect,
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
    Topic::PieceFail,
    Topic::Ban,
    Topic::Exists,
    Topic::IncomingRequest,
  ]);

  let recon = b.build();

  (
    recon,
    Receivers {
      peers,
      scheduler,
      stats,
    },
  )
}
