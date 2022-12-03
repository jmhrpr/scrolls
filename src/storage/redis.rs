use std::{str::FromStr, time::Duration};

use gasket::{
    error::AsWorkError,
    runtime::{spawn_stage, WorkOutcome},
};

use log::warn;
use redis::{Commands, ToRedisArgs};
use serde::Deserialize;

use crate::{bootstrap, crosscut, model};

type InputPort = gasket::messaging::TwoPhaseInputPort<model::StorageAction>;

impl ToRedisArgs for model::Value {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        match self {
            model::Value::String(x) => x.write_redis_args(out),
            model::Value::BigInt(x) => x.to_string().write_redis_args(out),
            model::Value::Cbor(x) => x.write_redis_args(out),
            model::Value::Json(x) => todo!("{}", x),
        }
    }
}

#[derive(Deserialize, Clone)]
pub struct Config {
    pub connection_params: String,
    pub cursor_key: Option<String>,
}

impl Config {
    pub fn bootstrapper(
        self,
        _chain: &crosscut::ChainWellKnownInfo,
        _intersect: &crosscut::IntersectConfig,
    ) -> Bootstrapper {
        Bootstrapper {
            config: self,
            input: Default::default(),
        }
    }

    pub fn cursor_key(&self) -> &str {
        self.cursor_key.as_deref().unwrap_or("_cursor")
    }
}

pub struct Bootstrapper {
    config: Config,
    input: InputPort,
}

impl Bootstrapper {
    pub fn borrow_input_port(&mut self) -> &'_ mut InputPort {
        &mut self.input
    }

    pub fn build_cursor(&self) -> Cursor {
        Cursor {
            config: self.config.clone(),
        }
    }

    pub fn spawn_stages(self, pipeline: &mut bootstrap::Pipeline) {
        let worker = Worker {
            config: self.config.clone(),
            connection: None,
            input: self.input,
            ops_count: Default::default(),
        };

        pipeline.register_stage(spawn_stage(
            worker,
            gasket::runtime::Policy {
                tick_timeout: Some(Duration::from_secs(600)),
                bootstrap_retry: gasket::retries::Policy {
                    max_retries: 20,
                    backoff_unit: Duration::from_secs(1),
                    backoff_factor: 2,
                    max_backoff: Duration::from_secs(60),
                },
                ..Default::default()
            },
            Some("redis"),
        ));
    }
}

pub struct Cursor {
    config: Config,
}

impl Cursor {
    pub fn last_point(&mut self) -> Result<Option<crosscut::PointArg>, crate::Error> {
        let mut connection = redis::Client::open(self.config.connection_params.clone())
            .and_then(|x| x.get_connection())
            .map_err(crate::Error::storage)?;

        let raw: Option<String> = connection
            .get(&self.config.cursor_key())
            .map_err(crate::Error::storage)?;

        let point = match raw {
            Some(x) => Some(crosscut::PointArg::from_str(&x)?),
            None => None,
        };

        Ok(point)
    }
}

pub struct Worker {
    config: Config,
    connection: Option<redis::Connection>,
    ops_count: gasket::metrics::Counter,
    input: InputPort,
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
            model::StorageAction::BlockStarting(_) => {
                // start redis transaction
                redis::cmd("MULTI")
                    .query(self.connection.as_mut().unwrap())
                    .or_restart()?;
            }

            // When rollbacking we need to know that the SetAdd created a new entry
            // otherwise we would have the situation where we we are adding a value
            // to the set which is already contained (so the set is unchanged)
            // but when we rollback we do the reverse of the add and remove the value
            // even though the value was already there before this add action.
            model::StorageAction::SetAdd(key, value) => {
                log::debug!("adding to set [{}], value [{}]", key, value);

                redis::cmd("SADD")
                    .arg(key)
                    .arg(value)
                    .query(self.connection.as_mut().unwrap())
                    .or_restart()?;
            }

            // We need to know whether or not the remove actually removed a value
            // or if the value didn't actually exist, to avoid re-adding a value
            // on rollback which wasn't actually present.
            model::StorageAction::SetRemove(key, value) => {
                log::debug!("removing from set [{}], value [{}]", key, value);

                self.connection
                    .as_mut()
                    .unwrap()
                    .srem(key, value)
                    .or_restart()?;
            }

            // We don't need to know if the value already existed because we just
            // treat 0 as the same as the value entry not existing (and remove them).
            model::StorageAction::SortedSetIncr(key, value, delta) => {
                log::debug!(
                    "sorted set incr [{}], value [{}], delta [{}]",
                    key,
                    value,
                    delta
                );

                self.connection
                    .as_mut()
                    .unwrap()
                    .zincr(&key, value, delta)
                    .or_restart()?;

                if delta < 0 {
                    // removal of 0 score members (garbage collection)
                    self.connection
                        .as_mut()
                        .unwrap()
                        .zrembyscore(&key, 0, 0)
                        .or_restart()?;
                }
            }

            // We have to disable score overwrites (or return a value like with getset)
            // otherwise we won't be able to return the score to what it was before
            // on a rollback.
            model::StorageAction::SortedSetAdd(key, member, score) => {
                log::debug!("sorted set add [{}] with score [{}]", key, score);

                redis::cmd("ZADD")
                    .arg(key)
                    .arg("NX") // no overwrites
                    .arg(score)
                    .arg(member)
                    .query(self.connection.as_mut().unwrap())
                    .or_restart()?;
            }

            // We need to store what the original value was, so we will use "get_set"
            // which will return the value being overwritten, and will can use this
            // overwritten value if we need to rollback.
            model::StorageAction::KeyValueSet(key, value) => {
                log::debug!("key value set [{}]", key);

                self.connection
                    .as_mut()
                    .unwrap()
                    .getset(key, value) // returns NIL if not overwriting
                    .or_restart()?;
            }

            // Use `GETDEL` so we can store the deleted key for rollback purposes.
            model::StorageAction::KeyValueDelete(key) => {
                log::debug!("key value set [{}]", key);

                redis::cmd("GETDEL")
                    .arg(key)
                    .query(self.connection.as_mut().unwrap())
                    .or_restart()?;
            },

            model::StorageAction::PNCounter(key, delta) => {
                log::debug!("increasing counter [{}], by [{}]", key, delta);

                self.connection
                    .as_mut()
                    .unwrap()
                    .incr(key, delta)
                    .or_restart()?;
            }

            // We need to rollback the cursor, but maybe we can just do this when
            // we are processing the blocks in reverse. TODO
            // For now we getset the cursor so we can store the overwritten value.
            // Alternatively we could just set the cursor to the point specified
            // in the rollback message, at the end of the redis transaction (if
            // the tx last alls blocks).
            model::StorageAction::BlockFinished(point) => {
                let cursor_str = crosscut::PointArg::from(point).to_string();

                self.connection
                    .as_mut()
                    .unwrap()
                    .getset(self.config.cursor_key(), &cursor_str)
                    .or_restart()?;

                log::info!(
                    "new cursor saved to redis {} {}",
                    &self.config.cursor_key(),
                    &cursor_str
                );

                // end redis transaction
                let tx_res: Vec<redis::Value> = redis::cmd("EXEC")
                    .query(self.connection.as_mut().unwrap())
                    .or_restart()?;

                warn!("exec return {:?}", tx_res);
            }

            // Rollback-only actions

            model::StorageAction::RollbackStarting(_) => {
                // start redis transaction
                redis::cmd("MULTI")
                    .query(self.connection.as_mut().unwrap())
                    .or_restart()?;
            }
            model::StorageAction::RollbackFinished(_point) => {
                // TODO set cursor to the preceeding block

                // end redis transaction
                redis::cmd("EXEC")
                    .query(self.connection.as_mut().unwrap())
                    .or_restart()?;
            },

            // Should only be used for undoing blocks because we can't rollback
            // the action due to not having information on the `score`.
            model::StorageAction::SortedSetRem(key, value) => {
                log::debug!("sorted set remove [{}]", key);

                self.connection
                    .as_mut()
                    .unwrap()
                    .zrem(&key, value)
                    .or_restart()?;
            }
        };

        self.ops_count.inc(1);
        self.input.commit();

        Ok(WorkOutcome::Partial)
    }

    fn bootstrap(&mut self) -> Result<(), gasket::error::Error> {
        self.connection = redis::Client::open(self.config.connection_params.clone())
            .and_then(|c| c.get_connection())
            .or_retry()?
            .into();

        Ok(())
    }

    fn teardown(&mut self) -> Result<(), gasket::error::Error> {
        Ok(())
    }
}
