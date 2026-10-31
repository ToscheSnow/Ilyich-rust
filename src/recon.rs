use std::{collections::HashMap, hash::Hash, sync::Arc};

use tokio::sync::{
  self,
  mpsc::{UnboundedReceiver, UnboundedSender},
};

#[derive(Clone)]
pub struct Recon<E> {
  listeners: Arc<HashMap<E, Vec<UnboundedSender<E>>>>,
}

impl<E: Eq + Hash + Clone> Recon<E> {
  fn announce(&self, event: E) {
    let listeners = self
      .listeners
      .get(&event)
      .expect("Only permitted torrent events");

    for listener in listeners {
      listener.send(event.clone());
    }
  }
}

pub struct ReconBuilder<E> {
  listeners: HashMap<E, Vec<UnboundedSender<E>>>,
}

impl<E: Eq + std::hash::Hash + Clone> ReconBuilder<E> {
  fn new() -> Self {
    ReconBuilder {
      listeners: HashMap::new(),
    }
  }

  fn subscribe(&mut self, event: E) -> UnboundedReceiver<E> {
    let (tx, rx) = sync::mpsc::unbounded_channel::<E>();
    let list = self.listeners.entry(event).or_default();

    list.push(tx);

    rx
  }

  fn build(self) -> Recon<E> {
    Recon {
      listeners: self.listeners.into(),
    }
  }
}
