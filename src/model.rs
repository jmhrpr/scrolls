use std::{collections::HashMap, fmt::Debug};

use pallas::{
    ledger::traverse::{Era, MultiEraBlock, MultiEraOutput, OutputRef},
    network::miniprotocols::Point,
};

use crate::Error;

#[derive(Debug, Clone)]
pub enum RawBlockPayload {
    RollForward(Vec<u8>),
    RollBack(Point),
}

impl RawBlockPayload {
    pub fn roll_forward(block: Vec<u8>) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::RollForward(block),
        }
    }

    pub fn roll_back(point: Point) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::RollBack(point),
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct BlockContext {
    utxos: HashMap<String, (Era, Vec<u8>)>,
}

impl BlockContext {
    pub fn import_ref_output(&mut self, key: &OutputRef, era: Era, cbor: Vec<u8>) {
        self.utxos.insert(key.to_string(), (era, cbor));
    }

    pub fn find_utxo(&self, key: &OutputRef) -> Result<MultiEraOutput, Error> {
        let (era, cbor) = self
            .utxos
            .get(&key.to_string())
            .ok_or_else(|| Error::missing_utxo(key))?;

        MultiEraOutput::decode(*era, cbor).map_err(crate::Error::cbor)
    }

    pub fn get_all_keys(&self) -> Vec<String> {
        self.utxos.keys().map(|x| x.clone()).collect()
    }
}

#[derive(Debug, Clone)]
pub enum EnrichedBlockPayload {
    RollForward(Vec<u8>, BlockContext),
    RollBack(Point),
}

impl EnrichedBlockPayload {
    pub fn roll_forward(block: Vec<u8>, ctx: BlockContext) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::RollForward(block, ctx),
        }
    }

    pub fn roll_back(point: Point) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::RollBack(point),
        }
    }
}

pub type Set = String;
pub type Member = String;
pub type Key = String;
pub type Delta = i64;
pub type Score = u64;

#[derive(Clone, Debug)]
pub enum Value {
    String(String),
    BigInt(i128),
    Cbor(Vec<u8>),
    Json(serde_json::Value),
}

impl From<String> for Value {
    fn from(x: String) -> Self {
        Value::String(x)
    }
}

impl From<Vec<u8>> for Value {
    fn from(x: Vec<u8>) -> Self {
        Value::Cbor(x)
    }
}

impl From<serde_json::Value> for Value {
    fn from(x: serde_json::Value) -> Self {
        Value::Json(x)
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum StorageAction {
    BlockStarting(Point),
    BlockFinished(Point),

    // make Value?
    SetAdd(Set, Member),
    SetRemove(Set, Member),

    SortedSetIncr(Set, Member, Delta),

    SortedSetAdd(Key, Value, Score),
    SortedSetRem(Key, Value),

    KeyValueSet(Key, Value),
    KeyValueDelete(Key),

    // TODO make sure Value is a generic not stringly typed
    PNCounter(Key, Delta),

    BlockUndoStarting(Point),
    BlockUndoFinished(Point),
}

impl StorageAction {
    pub fn block_starting(block: &MultiEraBlock) -> StorageAction {
        let hash = block.hash();
        let slot = block.slot();
        let point = Point::Specific(slot, hash.to_vec());

        StorageAction::BlockStarting(point)
    }

    pub fn set_add(prefix: Option<&str>, key: &str, member: String) -> StorageAction {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key),
            None => key.to_string(),
        };

        StorageAction::SetAdd(key, member)
    }

    pub fn set_remove(prefix: Option<&str>, key: &str, member: String) -> StorageAction {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key),
            None => key.to_string(),
        };

        StorageAction::SetRemove(key, member)
    }

    pub fn any_write_wins<K, V>(prefix: Option<&str>, key: K, value: V) -> StorageAction
    where
        K: ToString,
        V: Into<Value>,
    {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key.to_string()),
            None => key.to_string(),
        };

        StorageAction::KeyValueSet(key, value.into())
    }

    pub fn last_write_wins<V>(prefix: Option<&str>, key: &str, value: V, ts: Score) -> StorageAction
    where
        V: Into<Value>,
    {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key),
            None => key.to_string(),
        };

        StorageAction::SortedSetAdd(key, value.into(), ts)
    }

    pub fn block_finished(block: &MultiEraBlock) -> StorageAction {
        let hash = block.hash();
        let slot = block.slot();
        let point = Point::Specific(slot, hash.to_vec());
        StorageAction::BlockFinished(point)
    }

    /// Given a storage action, return a storage actions which does the inverse effects
    pub fn inverse(&self) -> StorageAction {
        match self.clone() {
            StorageAction::BlockStarting(point) => StorageAction::BlockUndoFinished(point),
            StorageAction::BlockFinished(point) => StorageAction::BlockUndoStarting(point),

            // TODO Do not allow overwrites (NX mode) in SetAdd, or alternatively store the overwritten value in rollback handler
            StorageAction::SetAdd(key, member) => StorageAction::SetRemove(key, member),
            // TODO Only if key existed (so one was actually removed)
            StorageAction::SetRemove(key, member) => StorageAction::SetAdd(key, member),

            // TODO We need to know if created an entry, or otherwise treat 0 scores same as none existent, or garbage collect it
            StorageAction::SortedSetIncr(key, value, delta) => {
                StorageAction::SortedSetIncr(key, value, delta * (-1))
            }

            // TODO Do not allow score overwrites (NX mode) SortedSetAdd
            StorageAction::SortedSetAdd(key, value, _score) => {
                StorageAction::SortedSetRem(key, value)
            }

            // TODO Do not allow overwrites (NX mode) in KeyValueSet, or store the overwritten value (getset)
            StorageAction::KeyValueSet(key, _value) => StorageAction::KeyValueDelete(key),

            StorageAction::PNCounter(key, delta) => StorageAction::PNCounter(key, delta * (-1)),

            // These actions cannot be rollbacked TODO error
            StorageAction::BlockUndoStarting(_) => unreachable!(),
            StorageAction::BlockUndoFinished(_) => unreachable!(),
            StorageAction::SortedSetRem(_, _) => unreachable!(),
            StorageAction::KeyValueDelete(_) => unreachable!(),
        }
    }
}
