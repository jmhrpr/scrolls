use pallas::ledger::traverse::MultiEraBlock;
use serde::Deserialize;

use crate::crosscut;
use crate::crosscut::epochs::block_epoch;
use crate::model::{StorageAction, Value};

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
}

pub struct Reducer {
    config: Config,
    chain: crosscut::ChainWellKnownInfo,
}

impl Reducer {
    pub fn current_epoch(
        &mut self,
        block: &MultiEraBlock,
        key: &str,
        actions: &mut Vec<StorageAction>,
    ) -> Result<(), gasket::error::Error> {
        let epoch_no = block_epoch(&self.chain, block);

        let action = StorageAction::KeyValueSet(
            format!("{}.{}", key, "epoch_no"),
            Value::BigInt(epoch_no as i128),
        );

        Ok(actions.push(action))
    }

    pub fn current_height(
        &mut self,
        block: &MultiEraBlock,
        key: &str,
        actions: &mut Vec<StorageAction>,
    ) -> Result<(), gasket::error::Error> {
        let action = StorageAction::KeyValueSet(
            format!("{}.{}", key, "height"),
            Value::BigInt(block.number() as i128),
        );

        Ok(actions.push(action))
    }

    pub fn current_slot(
        &mut self,
        block: &MultiEraBlock,
        key: &str,
        actions: &mut Vec<StorageAction>,
    ) -> Result<(), gasket::error::Error> {
        let action = StorageAction::KeyValueSet(
            format!("{}.{}", key, "slot_no"),
            Value::BigInt(block.slot() as i128),
        );

        Ok(actions.push(action))
    }

    pub fn current_block_hash(
        &mut self,
        block: &MultiEraBlock,
        key: &str,
        actions: &mut Vec<StorageAction>,
    ) -> Result<(), gasket::error::Error> {
        let action = StorageAction::KeyValueSet(
            format!("{}.{}", key, "block_hash"),
            Value::String(block.hash().to_string()),
        );

        Ok(actions.push(action))
    }

    pub fn current_block_era(
        &mut self,
        block: &MultiEraBlock,
        key: &str,
        actions: &mut Vec<StorageAction>,
    ) -> Result<(), gasket::error::Error> {
        let action = StorageAction::KeyValueSet(
            format!("{}.{}", key, "block_era"),
            Value::String(block.era().to_string()),
        );

        Ok(actions.push(action))
    }

    pub fn current_block_last_tx_hash(
        &mut self,
        block: &MultiEraBlock,
        key: &str,
        actions: &mut Vec<StorageAction>,
    ) -> Result<(), gasket::error::Error> {
        if !block.is_empty() {
            let action = StorageAction::KeyValueSet(
                format!("{}.{}", key, "first_transaction_hash"),
                Value::String(block.txs().first().unwrap().hash().to_string()),
            );

            actions.push(action);

            let action = StorageAction::KeyValueSet(
                format!("{}.{}", key, "last_transaction_hash"),
                Value::String(block.txs().last().unwrap().hash().to_string()),
            );

            actions.push(action);
        }

        Ok(())
    }

    pub fn current_block_last_tx_count(
        &mut self,
        block: &MultiEraBlock,
        key: &str,
        actions: &mut Vec<StorageAction>,
    ) -> Result<(), gasket::error::Error> {
        let action = StorageAction::KeyValueSet(
            format!("{}.{}", key, "transactions_count"),
            Value::BigInt(block.tx_count() as i128),
        );

        Ok(actions.push(action))
    }

    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        actions: &mut Vec<StorageAction>,
    ) -> Result<(), gasket::error::Error> {
        let def_key_prefix = "last_block";

        let key = match &self.config.key_prefix {
            Some(prefix) => format!("{}", prefix),
            None => format!("{}", def_key_prefix.to_string()),
        };

        self.current_epoch(block, &key, actions)?;
        self.current_height(block, &key, actions)?;
        self.current_slot(block, &key, actions)?;
        self.current_block_hash(block, &key, actions)?;
        self.current_block_era(block, &key, actions)?;
        self.current_block_last_tx_hash(block, &key, actions)?;
        self.current_block_last_tx_count(block, &key, actions)?;

        Ok(())
    }
}

impl Config {
    pub fn plugin(self, chain: &crosscut::ChainWellKnownInfo) -> super::Reducer {
        let reducer = Reducer {
            config: self,
            chain: chain.clone(),
        };

        super::Reducer::LastBlockParameters(reducer)
    }
}
