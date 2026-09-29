use std::collections::HashSet;

use crate::peer::Peer;

pub struct PieceManager {
  activePeers: HashSet<Peer>,
}
