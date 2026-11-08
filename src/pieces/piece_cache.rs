use bytes::Bytes;
use lru::LruCache;
use std::num::NonZeroUsize;

use crate::storage_manager::BlockMeta;

pub struct PieceCache {
  cache: LruCache<u32, Bytes>, // piece index -> whole piece data
}

impl PieceCache {
  pub fn new(max_pieces: usize) -> Self {
    Self {
      cache: LruCache::new(NonZeroUsize::new(max_pieces).unwrap()),
    }
  }

  // returns a block if its piece is cached
  pub fn get_block(&mut self, block_meta: &BlockMeta) -> Option<Bytes> {
    let &BlockMeta {
      piece_idx,
      offset,
      length,
    } = block_meta;
    let data = self.cache.get(&piece_idx)?; // also marks it "recently used"
    if offset + length > data.len() as u32 {
      return None;
    } // bad request, don't panic
    let start = offset as usize;
    let end = start + length as usize;
    Some(data.slice(start..end)) // zero-copy, just a refcount bump
  }

  pub fn put_piece(&mut self, piece: u32, data: Bytes) {
    self.cache.put(piece, data); // evicts the oldest if full
  }
}
