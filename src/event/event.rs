use crate::{peers::peer::PeerHandle, pieces::piece_manager::Piece};

#[derive(Clone)]
pub enum TorrentEvent {
  // peer
  Connect(PeerHandle),
  SeederConnect(PeerHandle),
  Disconnect(PeerHandle),
  Choke(PeerHandle),
  Unchoke(PeerHandle),
  Have(PeerHandle, u32),
  Bitfield,
  Ban(PeerHandle),
  // piece
  PieceComplete(u32, u32),
  BlockReceive(Piece),
  PieceFail(u32),
  BlockUpload(u32),
  Exists(u32),
  // lifecycle
  DownloadComplete,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Topic {
  Connect,
  SeederConnect,
  Disconnect,
  Choke,
  Unchoke,
  Have,
  Bitfield,
  Ban,
  IncomingRequest,
  PieceComplete,
  BlockReceive,
  PieceFail,
  BlockUpload,
  Exists,
  DownloadComplete,
}

impl crate::recon::Event for TorrentEvent {
  type Topic = Topic;

  fn topic(&self) -> Topic {
    match self {
      TorrentEvent::Connect(..) => Topic::Connect,
      TorrentEvent::SeederConnect(..) => Topic::SeederConnect,
      TorrentEvent::Disconnect(..) => Topic::Disconnect,
      TorrentEvent::Choke(..) => Topic::Choke,
      TorrentEvent::Unchoke(..) => Topic::Unchoke,
      TorrentEvent::Have(..) => Topic::Have,
      TorrentEvent::Bitfield => Topic::Bitfield,
      TorrentEvent::Ban(..) => Topic::Ban,
      TorrentEvent::PieceComplete(..) => Topic::PieceComplete,
      TorrentEvent::BlockReceive(..) => Topic::BlockReceive,
      TorrentEvent::PieceFail(..) => Topic::PieceFail,
      TorrentEvent::BlockUpload(..) => Topic::BlockUpload,
      TorrentEvent::Exists(..) => Topic::Exists,
      TorrentEvent::DownloadComplete => Topic::DownloadComplete,
    }
  }
}
