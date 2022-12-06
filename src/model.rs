use std::{collections::HashMap, fmt::Debug};

use pallas::{
    ledger::traverse::{Era, MultiEraBlock, MultiEraOutput, OutputRef},
    network::miniprotocols::Point,
};

use crate::Error;

#[derive(Debug, Clone)]
pub enum RawBlockPayload {
    RollForward(Point, Vec<u8>),
    RollBack(Point),
}

impl RawBlockPayload {
    pub fn roll_forward(point: Point, block: Vec<u8>) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::RollForward(point, block),
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
    RollForward(Point, Vec<u8>, BlockContext),
    RollBack(Point),
}

impl EnrichedBlockPayload {
    pub fn roll_forward(
        point: Point,
        block: Vec<u8>,
        ctx: BlockContext,
    ) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::RollForward(point, block, ctx),
        }
    }

    pub fn roll_back(point: Point) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::RollBack(point),
        }
    }
}

#[derive(Debug, Clone)]
pub enum StorageActionPayload {
    RollForward(Point, Vec<StorageAction>),
    RollBack(Point, Vec<StorageAction>),
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

// TODO Investigate Value/Member usage
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum StorageAction {
    BlockStarting(Point),
    BlockFinished(Point),

    SetAdd(Set, Member),
    SetRemove(Set, Member),

    SortedSetIncr(Set, Member, Delta),

    SortedSetAdd(Key, Value, Score),
    SortedSetRem(Key, Value),

    KeyValueSet(Key, Value),
    KeyValueDelete(Key),

    PNCounter(Key, Delta),

    RollbackStarting(Point),
    RollbackFinished(Point),
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum StorageActionResult {
    MemberExistence(bool),            // SetAdd, SetRem
    ValueDestruction(Option<String>), // KeyValueSet, KeyValueDelete
    NotApplicable,                    // PNCounter, SortedSetIncr, SortedSetAdd
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

    pub fn last_write_wins<V>(
        prefix: Option<&str>,
        key: &str,
        value: V,
        score: Score,
    ) -> StorageAction
    where
        V: Into<Value>,
    {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key),
            None => key.to_string(),
        };

        StorageAction::SortedSetAdd(key, value.into(), score)
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
            StorageAction::BlockStarting(_point) => todo!(),
            StorageAction::BlockFinished(_point) => todo!(),

            // Use return value to know if member already existed
            StorageAction::SetAdd(key, member) => StorageAction::SetRemove(key, member),
            // Use return value to know if a member existed and was therefore deleted
            StorageAction::SetRemove(key, member) => StorageAction::SetAdd(key, member),

            // 0 scores must be treated the same as missing entries
            StorageAction::SortedSetIncr(key, value, delta) => {
                StorageAction::SortedSetIncr(key, value, delta * (-1))
            }

            // Do not allow score overwrites (NX mode) SortedSetAdd
            StorageAction::SortedSetAdd(key, value, _score) => {
                StorageAction::SortedSetRem(key, value)
            }

            // Use return value to know the overwritten member Store the overwritten value (getset)
            StorageAction::KeyValueSet(key, _value) => StorageAction::KeyValueDelete(key),

            StorageAction::PNCounter(key, delta) => StorageAction::PNCounter(key, delta * (-1)),

            // These actions cannot be rollbacked TODO error
            StorageAction::RollbackStarting(_) => unreachable!(),
            StorageAction::RollbackFinished(_) => unreachable!(),
            StorageAction::SortedSetRem(_, _) => unreachable!(),
            StorageAction::KeyValueDelete(_) => unreachable!(),
        }
    }
}
