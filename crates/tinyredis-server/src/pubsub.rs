use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::broadcast;
use tracing::debug;

const CHANNEL_CAPACITY: usize = 1024;

pub struct PubSubHub {
    channels: DashMap<String, broadcast::Sender<String>>,
}

impl Default for PubSubHub {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSubHub {
    pub fn new() -> Self {
        Self {
            channels: DashMap::new(),
        }
    }

    pub fn subscribe(&self, channel: &str) -> broadcast::Receiver<String> {
        let sender = self
            .channels
            .entry(channel.to_string())
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0)
            .clone();
        sender.subscribe()
    }

    pub fn unsubscribe(&self, channel: &str) {
        if let Some(entry) = self.channels.get(channel) {
            if entry.receiver_count() == 0 {
                self.channels.remove(channel);
            }
        }
    }

    pub fn publish(&self, channel: &str, message: &str) -> usize {
        match self.channels.get(channel) {
            Some(sender) => {
                let count = sender.receiver_count();
                if sender.send(message.to_string()).is_err() {
                    debug!(channel, "publish with no active subscribers");
                    return 0;
                }
                count
            }
            None => 0,
        }
    }
}

pub type SharedPubSubHub = Arc<PubSubHub>;
