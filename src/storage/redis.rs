use std::{
    str::{from_utf8, FromStr},
    time::Duration,
};

use gasket::{
    error::AsWorkError,
    runtime::{spawn_stage, WorkOutcome},
};

use log::warn;
use redis::{Commands, ToRedisArgs, Value::*};
use serde::Deserialize;

use crate::{
    bootstrap, crosscut,
    model::{
        self,
        StorageAction::{self, *},
        StorageActionPayload,
    },
    rollback::buffer::RollbackBuffer,
};

type InputPort = gasket::messaging::TwoPhaseInputPort<model::StorageActionPayload>;

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

// SetAdd(K, X) + bool for if the value already existed or not = SetRem(K, X) if the value did not exist or nothing if it already existed
// SetRemove(K, X) + bool for if the value existed or not = SetAdd(K, X) if the value existed or nothing if it didn't

// SortedSetIncr(K, X, S) + nothing = SortedSetIncr(K, X, -1 * S) (0 score entries should be removed after)
// SortedSetAdd(K, X, S) + nothing (don't allow overwrites) = SortedSetRem(K, X)
// SortedSetRem(K, X) = not supported

// KeyValueSet(K, X) + Option<string/value which was overwritten> => KeyValueDelete(K, X) if None (it didnt exist before), or KeyValueSet(K, Y) if Some(Y) (it existed before with value Y)
// KeyValueDelete(K) + Option<string/value of the deleted key> => Nothing if None (nothing was deleted) or KeyValueSet(K, Y) if Some(Y) was the value of the deleted key

// PNCounter(K, X, D) + nothing => PNCounter(K, X, -1 * D)

// Inverse Storage Action by taking StorageAction and its response

// impl StorageActionResult {
//     pub fn from_redis_value(action: StorageAction, value: redis::Value) -> StorageActionResult {
//         match (action, value) {
//             // SADD returns the number of elements added to the set, so if
//             // the response is 0 it is because the element was already present.
//             (a @ StorageAction::SetAdd(_, _), redis::Value::Int(0)) => StorageActionResult::MemberExistence(true), // do nothing
//             (a @ StorageAction::SetAdd(_, _), redis::Value::Int(1)) => StorageActionResult::MemberExistence(false),
//             // SREM returns the number of elements removed from the set, so if
//             // the response is 0 it is because the element was not present.
//             (a @ StorageAction::SetRemove(_, _), redis::Value::Int(0)) => StorageActionResult::MemberExistence(true), // do nothing
//             (a @ StorageAction::SetRemove(_, _), redis::Value::Int(1)) => StorageActionResult::MemberExistence(true),

//             //
//         }
//     }
// }

/// Given a storage action and the corresponding Redis value which was returned
/// for the Redis query associated with the storage action, use the action and
/// return value to figure out what storage action will reverse the effects.
pub fn inverse_action(action: StorageAction, value: redis::Value) -> Option<StorageAction> {
    match (action, value) {
        // SADD returns the number of elements added to the set, so if
        // the response is 0 it is because the element was already present.
        // If it wasn't already present we undo the action by removing the
        // member from the set, otherwise do nothing.
        (SetAdd(_, _), Int(0)) => None, // do nothing
        (SetAdd(k, m), Int(1)) => Some(SetRemove(k, m)),

        // SREM returns the number of elements removed from the set, so if
        // the response is 0 it is because the element was not present.
        (SetRemove(_, _), Int(0)) => None, // do nothing
        (SetRemove(k, m), Int(1)) => Some(SetAdd(k, m)),

        // Decrement the sorted set member by the same amount it was incremented.
        (SortedSetIncr(k, m, d), _) => Some(SortedSetIncr(k, m, (-1) * d)),

        // We do not allow overwrites in sorted set adds, so a return of 1 means
        // the member was added and a return of 0 means it was not added.
        (SortedSetAdd(_k, _m, _s), Int(0)) => None,
        (SortedSetAdd(k, m, _s), Int(1)) => Some(StorageAction::SortedSetRem(k, m)),

        // No value was overwritten so simply delete the key that was added.
        (KeyValueSet(k, _v), Nil) => Some(StorageAction::KeyValueDelete(k)),

        // The set resulted in a value being overwritten, so set the key back to
        // what it was previously: the overwritten value `B`.
        (KeyValueSet(k, _v), Data(bs)) => Some(KeyValueSet(k, bs.into())),

        // The action deleted a key which did not exist, so we don't need to do
        // anything to invert it.
        (KeyValueDelete(_k), Nil) => None,
        // The action deleted a key which existed and had value `B`. Set the key
        // `K` to `B`.
        (KeyValueDelete(k), Data(bs)) => Some(KeyValueSet(k, bs.into())),

        // Decrement the counter by the amount be incremented it.
        (StorageAction::PNCounter(k, d), _) => Some(StorageAction::PNCounter(k, (-1) * d)),

        (StorageAction::SortedSetRem(_, _), _) => {
            unreachable!("SortedSetRem is not compatible with graceful rollback")
        }

        (action, result) => unreachable!(
            "tried to reverse action {:?} and result {:?}",
            action, result
        ),
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
            rollback_buffer: Default::default(),
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
    rollback_buffer: RollbackBuffer<StorageResult>,
}

// A storage action paired with the result from the storage after applying it
type StorageResult = Vec<(StorageAction, redis::Value)>;

impl Worker {
    /// Given a list of desired storage actions, perform the appropriate Redis
    /// command.
    fn apply_actions(&mut self, actions: &Vec<StorageAction>) -> Result<(), gasket::error::Error> {
        for action in actions {
            match action {
                // When rollbacking we need to know that the SetAdd created a new entry
                // otherwise we would have the situation where we we are adding a value
                // to the set which is already contained (so the set is unchanged)
                // but when we rollback we do the reverse of the add and remove the value
                // even though the value was already there before this add action.
                StorageAction::SetAdd(key, value) => {
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
                StorageAction::SetRemove(key, value) => {
                    log::debug!("removing from set [{}], value [{}]", key, value);

                    self.connection
                        .as_mut()
                        .unwrap()
                        .srem(key, value)
                        .or_restart()?;
                }

                // We don't need to know if the value already existed because we just
                // treat 0 as the same as the value entry not existing (and remove them).
                StorageAction::SortedSetIncr(key, value, delta) => {
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

                    // TODO double action bad
                    // if *delta < 0 {
                    //     // removal of 0 score members (garbage collection)
                    //     self.connection
                    //         .as_mut()
                    //         .unwrap()
                    //         .zrembyscore(&key, 0, 0)
                    //         .or_restart()?;
                    // }
                }

                // We have to disable score overwrites (or return a value like with getset)
                // otherwise we won't be able to return the score to what it was before
                // on a rollback.
                StorageAction::SortedSetAdd(key, member, score) => {
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
                StorageAction::KeyValueSet(key, value) => {
                    log::debug!("key value set [{}]", key);

                    self.connection
                        .as_mut()
                        .unwrap()
                        .getset(key, value) // returns NIL if not overwriting
                        .or_restart()?;
                }

                // Use `GETDEL` so we can store the deleted key for rollback purposes.
                StorageAction::KeyValueDelete(key) => {
                    log::debug!("key value set [{}]", key);

                    redis::cmd("GETDEL")
                        .arg(key)
                        .query(self.connection.as_mut().unwrap())
                        .or_restart()?;
                }

                StorageAction::PNCounter(key, delta) => {
                    log::debug!("increasing counter [{}], by [{}]", key, delta);

                    self.connection
                        .as_mut()
                        .unwrap()
                        .incr(key, delta)
                        .or_restart()?;
                }

                // // Rollback-only actions
                // StorageAction::RollbackStarting(_) => {
                //     // start redis transaction
                //     redis::cmd("MULTI")
                //         .query(self.connection.as_mut().unwrap())
                //         .or_restart()?;
                // }
                // StorageAction::RollbackFinished(_point) => {
                //     // TODO set cursor to the preceeding block

                //     // end redis transaction
                //     redis::cmd("EXEC")
                //         .query(self.connection.as_mut().unwrap())
                //         .or_restart()?;
                // }

                // Should only be used for undoing blocks because we can't rollback
                // the action due to not having information on the `score`.
                StorageAction::SortedSetRem(key, value) => {
                    log::debug!("sorted set remove [{}]", key);

                    self.connection
                        .as_mut()
                        .unwrap()
                        .zrem(&key, value)
                        .or_restart()?;
                }
            };
        }

        Ok(())
    }
}

impl gasket::runtime::Worker for Worker {
    fn metrics(&self) -> gasket::metrics::Registry {
        gasket::metrics::Builder::new()
            .with_counter("storage_ops", &self.ops_count)
            .build()
    }

    fn work(&mut self) -> gasket::runtime::WorkResult {
        // receive the MsgRollForward/Backwards (and list of storage actions)
        let msg = self.input.recv_or_idle()?;

        match msg.payload {
            StorageActionPayload::RollForward(point, actions) => {
                // start transaction
                // apply all reducer actions
                // potentially separately set cursor
                // end transaction get list of returned responses
                // zip actions to responses, modifying the responses into StorageActionResults

                // === BLOCK STARTING ===

                // start redis transaction (block starting)
                redis::cmd("MULTI")
                    .query(self.connection.as_mut().unwrap())
                    .or_restart()?;

                // === APPLY REDUCER STORAGE ACTIONS ===

                // execute the actions
                self.apply_actions(&actions)?;

                // === BLOCK FINISHED ===

                // block finished (TODO function)
                let cursor_str = crosscut::PointArg::from(point.clone()).to_string();

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
                let responses: Vec<redis::Value> = redis::cmd("EXEC")
                    .query(self.connection.as_mut().unwrap())
                    .or_restart()?;

                // warn!("{} actions {} responses", actions.len(), responses.len());

                // warn!("exec return {:?}", responses);

                let actions_and_responses: Vec<(StorageAction, redis::Value)> =
                    actions.into_iter().zip(responses).collect();

                self.rollback_buffer.add_block(point, actions_and_responses);

                warn!("{:?}", self.rollback_buffer)

                // warn!("anr {:?} anr len {}", actions_and_responses, actions_and_responses.len());
            }
            StorageActionPayload::RollBack(point, _) => {
                warn!("rollback requested to {:?}", point)
                // TODO pull the actions and associated StorageActionResults from rollback buffer
                // in a transaction undo each of the actions and set the cursor to the rollback point
            }
        }

        // TODO fix
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
