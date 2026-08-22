use std::collections::HashSet;

use crate::command::Command;
use crate::database::Database;
use crate::error::{CoreError, CoreResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransactionState {
    #[default]
    Idle,
    Multi,
}

#[derive(Debug, Default)]
pub struct TransactionManager {
    state: TransactionState,
    queue: Vec<Command>,
    watched: HashSet<String>,
    snapshot: Option<Database>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn state(&self) -> TransactionState {
        self.state
    }

    pub fn multi(&mut self) -> CoreResult<()> {
        if self.state == TransactionState::Multi {
            return Err(CoreError::Transaction("MULTI called inside MULTI".into()));
        }
        self.state = TransactionState::Multi;
        self.queue.clear();
        Ok(())
    }

    pub fn queue(&mut self, cmd: Command) -> CoreResult<()> {
        if self.state != TransactionState::Multi {
            return Err(CoreError::Transaction("not in MULTI".into()));
        }
        self.queue.push(cmd);
        Ok(())
    }

    pub fn watch(&mut self, keys: &[String], db: &Database) -> CoreResult<()> {
        if self.snapshot.is_none() {
            self.snapshot = Some(db.clone());
        }
        self.watched.extend(keys.iter().cloned());
        Ok(())
    }

    pub fn unwatch(&mut self) {
        self.watched.clear();
        self.snapshot = None;
    }

    pub fn discard(&mut self) -> CoreResult<()> {
        if self.state != TransactionState::Multi {
            return Err(CoreError::Transaction("not in MULTI".into()));
        }
        self.reset();
        Ok(())
    }

    pub fn exec(&mut self, db: &mut Database) -> CoreResult<Vec<Command>> {
        if self.state != TransactionState::Multi {
            return Err(CoreError::Transaction("not in MULTI".into()));
        }
        if let Some(snapshot) = &self.snapshot {
            for key in &self.watched {
                let before = snapshot.get(key).ok().flatten();
                let after = db.get(key).ok().flatten();
                if before != after {
                    self.reset();
                    return Err(CoreError::WatchConflict);
                }
            }
        }
        let cmds = std::mem::take(&mut self.queue);
        self.reset();
        Ok(cmds)
    }

    fn reset(&mut self) {
        self.state = TransactionState::Idle;
        self.queue.clear();
        self.watched.clear();
        self.snapshot = None;
    }
}
