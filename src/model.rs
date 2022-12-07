use std::{collections::HashMap, fmt::Debug};

use pallas::{
    ledger::traverse::{Era, MultiEraOutput, OutputRef},
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
    SetAdd(Set, Member),
    SetRemove(Set, Member),

    SortedSetIncr(Set, Member, Delta),

    SortedSetAdd(Key, Value, Score),
    SortedSetRem(Key, Value),

    KeyValueSet(Key, Value),
    KeyValueDelete(Key),

    PNCounter(Key, Delta),
}

// #[derive(Clone, Debug)]
// #[non_exhaustive]
// pub enum StorageActionResult {
//     MemberExistence(bool),            // SetAdd, SetRem
//     ValueDestruction(Option<String>), // KeyValueSet, KeyValueDelete
//     NotApplicable,                    // PNCounter, SortedSetIncr, SortedSetAdd
// }

impl StorageAction {
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
}
