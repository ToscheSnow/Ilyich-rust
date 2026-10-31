use std::{
  collections::{HashMap, HashSet},
  hash::Hash,
  sync::Arc,
};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

/// Implemented by your event enum: "which topic am I?"
pub trait Event: Clone {
  type Topic: Eq + Hash + Copy;
  fn topic(&self) -> Self::Topic;
}

/// Announce-only handle. Cheap to clone (it's an Arc), hand it to anyone.
#[derive(Clone)]
pub struct Recon<E: Event> {
  routes: Arc<HashMap<E::Topic, Box<[UnboundedSender<E>]>>>,
}

impl<E: Event> Recon<E> {
  /// One hash lookup, then a send into each interested channel. No locks.
  pub fn announce(&self, event: E) {
    if let Some(subs) = self.routes.get(&event.topic()) {
      for tx in subs.iter() {
        // receiver dropped? ignore
        let _ = tx.send(event.clone());
      }
    }
  }
}

pub struct ReconBuilder<E: Event> {
  routes: HashMap<E::Topic, Vec<UnboundedSender<E>>>,
}

impl<E: Event> ReconBuilder<E> {
  pub fn new() -> Self {
    Self {
      routes: HashMap::new(),
    }
  }

  /// One receiver, many topics.
  pub fn subscribe(&mut self, topics: impl IntoIterator<Item = E::Topic>) -> UnboundedReceiver<E> {
    let (tx, rx) = mpsc::unbounded_channel();
    let unique: HashSet<_> = topics.into_iter().collect(); // no double delivery
    for topic in unique {
      self.routes.entry(topic).or_default().push(tx.clone());
    }
    rx
  }

  /// Freezes the routing table. Consumes the builder, so nobody can subscribe afterwards.
  pub fn build(self) -> Recon<E> {
    let routes = self
      .routes
      .into_iter()
      .map(|(topic, subs)| (topic, subs.into_boxed_slice()))
      .collect();
    Recon {
      routes: Arc::new(routes),
    }
  }
}
