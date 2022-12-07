use std::{str::FromStr, time::Duration};

use gasket::{
    error::AsWorkError,
    runtime::{spawn_stage, WorkOutcome},
};

use log::{debug, trace};
use pallas::network::miniprotocols::Point;
use redis::{Commands, ToRedisArgs, Value::*};
use serde::Deserialize;

use crate::{
    bootstrap, crosscut,
    model::{
        self,
        StorageAction::{self, *},
        StorageActionPayload,
    },
    prelude::AppliesPolicy,
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

// Inverse Storage Action by taking StorageAction and its response TODO

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
            "couldn't reverse action {:?} with result {:?}",
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
    pub fn bootstrapper(self, policy: &crosscut::policies::RuntimePolicy) -> Bootstrapper {
        Bootstrapper {
            config: self,
            policy: policy.clone(),
            input: Default::default(),
        }
    }

    pub fn cursor_key(&self) -> &str {
        self.cursor_key.as_deref().unwrap_or("_cursor")
    }
}

pub struct Bootstrapper {
    config: Config,
    policy: crosscut::policies::RuntimePolicy,
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
            policy: self.policy.clone(),
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

    // TODO set cursor func
}

pub struct Worker {
    config: Config,
    policy: crosscut::policies::RuntimePolicy,
    connection: Option<redis::Connection>,
    ops_count: gasket::metrics::Counter,
    input: InputPort,
    rollback_buffer: RollbackBuffer<StorageResult>,
}

// A storage action paired with the result from the storage after applying it
type StorageResult = Vec<(StorageAction, redis::Value)>;

impl Worker {
    fn block_starting(&mut self) -> Result<(), gasket::error::Error> {
        // start redis transaction (block starting)
        redis::cmd("MULTI")
            .query(self.connection.as_mut().unwrap())
            .or_restart()?;

        Ok(())
    }

    fn block_finished(&mut self, point: &Point) -> Result<Vec<redis::Value>, gasket::error::Error> {
        // update cursor
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

        // end redis transaction, store responses
        let responses: Vec<redis::Value> = redis::cmd("EXEC")
            .query(self.connection.as_mut().unwrap())
            .or_restart()?;

        // return redis query responses
        Ok(responses)
    }

    fn rollback_starting(&mut self) -> Result<(), gasket::error::Error> {
        // start redis transaction to contain inverse actions for all rollbacked
        // blocks
        redis::cmd("MULTI")
            .query(self.connection.as_mut().unwrap())
            .or_restart()?;

        Ok(())
    }

    fn rollback_finished(&mut self, point: &Point) -> Result<(), gasket::error::Error> {
        // set cursor back to the rollback point
        let cursor_str = crosscut::PointArg::from(point.clone()).to_string();

        self.connection
            .as_mut()
            .unwrap()
            .getset(self.config.cursor_key(), &cursor_str)
            .or_restart()?;

        log::info!(
            "rollbacked cursor saved to redis {} {}",
            &self.config.cursor_key(),
            &cursor_str
        );

        // end transaction (end rollback)
        redis::cmd("EXEC")
            .query(self.connection.as_mut().unwrap())
            .or_restart()?;

        Ok(())
    }

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
            // TODO block cbor? height?
            StorageActionPayload::RollForward(point, actions) => {
                debug!(
                    "performing {} storage actions to process block {:?}",
                    actions.len(),
                    point
                );

                // block starting (start redis transaction)
                self.block_starting()?;

                // execute the actions from the reducer stage
                self.apply_actions(&actions)?;

                // block finished (update cursor, end redis transaction)
                let responses = self.block_finished(&point)?;

                // update the storage operation counter
                self.ops_count.inc(actions.len() as u64);

                // pair the storage actions with the corresponding value which
                // was returned from redis for each action
                let actions_and_responses: Vec<(StorageAction, redis::Value)> =
                    actions.into_iter().zip(responses).collect();

                trace!(
                    "{} actions/responses: {:?}",
                    actions_and_responses.len(),
                    actions_and_responses
                );

                // push the point, the storage actions and their corresponding
                // return values onto the front of the rollback buffer
                self.rollback_buffer.add_block(point, actions_and_responses);

                trace!(
                    "redis rollback buffer (len {}): {:?}",
                    self.rollback_buffer.len(),
                    self.rollback_buffer
                );
            }
            StorageActionPayload::RollBack(point, _) => {
                trace!(
                    "rollback buffer before: {} {:?}",
                    self.rollback_buffer.len(),
                    self.rollback_buffer
                );

                // Fetch the rollbacked blocks and the corresponding storage actions and associated
                // responses from Redis.
                let points_and_results = self
                    .rollback_buffer
                    .rollback_to_point(&point)
                    .map_err(crate::Error::rollback)
                    .apply_policy(&self.policy)
                    .or_panic()?;

                // apply the error policy for unhandleable rollbacks
                let points_and_results = match points_and_results {
                    Some(x) => x,
                    None => return Ok(gasket::runtime::WorkOutcome::Partial),
                };

                trace!(
                    "rollback buffer after: {} {:?}",
                    self.rollback_buffer.len(),
                    self.rollback_buffer
                );

                // Create a list of inverse actions, each of which reverses a storage action
                // that occured as a result of a block which has been rollbacked. The first
                // inverse action in the vec corresponds to the most recently applied action,
                // that is the last action performed by the most recent block.
                let inverse_actions: Vec<StorageAction> = points_and_results
                    .into_iter()
                    .flat_map(|point| point.result.into_iter().rev())
                    .flat_map(|(a, r)| inverse_action(a, r))
                    .collect();

                debug!(
                    "performing {} inverse actions to rollback to point {:?}",
                    inverse_actions.len(),
                    point
                );

                // start rollback (start redis transaction)
                self.rollback_starting()?;

                // apply all the inverse actions
                self.apply_actions(&inverse_actions)?;

                // finish rollback (set cursor to rollback point, end redis transaction)
                self.rollback_finished(&point)?;

                // count the number of storage actions which were performed
                self.ops_count.inc(inverse_actions.len() as u64);
            }
        }

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
