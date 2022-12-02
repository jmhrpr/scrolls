use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use gasket::runtime::{spawn_stage, WorkOutcome};

use serde::Deserialize;

use crate::{bootstrap, crosscut, model};

type InputPort = gasket::messaging::TwoPhaseInputPort<model::StorageAction>;

#[derive(Deserialize, Clone)]
pub struct Config {}

impl Config {
    pub fn bootstrapper(self) -> Bootstrapper {
        Bootstrapper {
            input: Default::default(),
            last_point: Arc::new(Mutex::new(None)),
        }
    }
}

pub struct Bootstrapper {
    input: InputPort,
    last_point: Arc<Mutex<Option<crosscut::PointArg>>>,
}

impl Bootstrapper {
    pub fn borrow_input_port(&mut self) -> &'_ mut InputPort {
        &mut self.input
    }

    pub fn build_cursor(&mut self) -> Cursor {
        Cursor {
            last_point: self.last_point.clone(),
        }
    }

    pub fn spawn_stages(self, pipeline: &mut bootstrap::Pipeline) {
        let worker = Worker {
            input: self.input,
            ops_count: Default::default(),
            last_point: self.last_point.clone(),
        };

        pipeline.register_stage(spawn_stage(
            worker,
            gasket::runtime::Policy {
                tick_timeout: Some(Duration::from_secs(600)),
                ..Default::default()
            },
            Some("skip"),
        ));
    }
}

pub struct Cursor {
    last_point: Arc<Mutex<Option<crosscut::PointArg>>>,
}

impl Cursor {
    pub fn last_point(&self) -> Result<Option<crosscut::PointArg>, crate::Error> {
        let value = self.last_point.lock().unwrap();
        Ok(value.clone())
    }
}

pub struct Worker {
    ops_count: gasket::metrics::Counter,
    input: InputPort,
    last_point: Arc<Mutex<Option<crosscut::PointArg>>>,
}

impl gasket::runtime::Worker for Worker {
    fn metrics(&self) -> gasket::metrics::Registry {
        gasket::metrics::Builder::new()
            .with_counter("storage_ops", &self.ops_count)
            .build()
    }

    fn work(&mut self) -> gasket::runtime::WorkResult {
        let msg = self.input.recv_or_idle()?;

        match msg.payload {
            model::StorageAction::BlockStarting(point) => {
                log::debug!("block started {:?}", point);
            }
            model::StorageAction::SetAdd(key, value) => {
                log::debug!("adding to set [{}], value [{}]", key, value);
            }
            model::StorageAction::SetRemove(key, value) => {
                log::debug!("removing from set [{}], value [{}]", key, value);
            }
            model::StorageAction::SortedSetAdd(key, _, score) => {
                log::debug!("adding to set [{}], score [{}]", key, score);
            }
            model::StorageAction::SortedSetRem(key, _) => {
                log::debug!("removing from sorted set [{}]", key,);
            }
            model::StorageAction::SortedSetIncr(key, _, delta) => {
                log::debug!("last write for [{}], delta [{}]", key, delta);
            }
            model::StorageAction::KeyValueSet(key, _) => {
                log::debug!("key value set [{}]", key);
            }
            model::StorageAction::KeyValueDelete(key) => {
                log::debug!("key value delete [{}]", key);
            }
            model::StorageAction::PNCounter(key, value) => {
                log::debug!("increasing counter [{}], by [{}]", key, value);
            }
            model::StorageAction::BlockFinished(point) => {
                log::debug!("block finished {:?}", point);
                let mut last_point = self.last_point.lock().unwrap();
                *last_point = Some(crosscut::PointArg::from(point));
            }
            model::StorageAction::BlockUndoStarting(point) => {
                log::debug!("block undo starting {:?}", point);
            }
            model::StorageAction::BlockUndoFinished(point) => {
                log::debug!("block undo finishing {:?}", point);
            }
        };

        self.ops_count.inc(1);
        self.input.commit();
        Ok(WorkOutcome::Partial)
    }
}
