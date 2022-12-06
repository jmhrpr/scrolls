use pallas::ledger::traverse::MultiEraOutput;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraTx, OutputRef};
use serde::Deserialize;

use crate::model::StorageAction;
use crate::{crosscut, model, prelude::*};

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub filter: Option<Vec<String>>,
}

pub struct Reducer {
    config: Config,
    policy: crosscut::policies::RuntimePolicy,
}

impl Reducer {
    fn process_consumed_txo(
        &mut self,
        ctx: &model::BlockContext,
        input: &OutputRef,
        actions: &mut Vec<StorageAction>,
    ) -> Result<(), gasket::error::Error> {
        let utxo = ctx.find_utxo(input).apply_policy(&self.policy).or_panic()?;

        let utxo = match utxo {
            Some(x) => x,
            None => return Ok(()),
        };

        let address = utxo.address().map(|x| x.to_string()).or_panic()?;

        if let Some(addresses) = &self.config.filter {
            if let Err(_) = addresses.binary_search(&address) {
                return Ok(());
            }
        }

        let action = StorageAction::set_remove(
            self.config.key_prefix.as_deref(),
            &address,
            input.to_string(),
        );

        Ok(actions.push(action))
    }

    fn process_produced_txo(
        &mut self,
        tx: &MultiEraTx,
        tx_output: &MultiEraOutput,
        output_idx: usize,
        actions: &mut Vec<StorageAction>,
    ) -> Result<(), gasket::error::Error> {
        let tx_hash = tx.hash();
        let address = tx_output
            .address()
            .map(|addr| addr.to_string())
            .or_panic()?;

        if let Some(addresses) = &self.config.filter {
            if let Err(_) = addresses.binary_search(&address) {
                return Ok(());
            }
        }

        let action = StorageAction::set_add(
            self.config.key_prefix.as_deref(),
            &address,
            format!("{}#{}", tx_hash, output_idx),
        );

        Ok(actions.push(action))
    }

    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        ctx: &model::BlockContext,
        actions: &mut Vec<StorageAction>,
    ) -> Result<(), gasket::error::Error> {
        for tx in block.txs().into_iter() {
            for consumed in tx.consumes().iter().map(|i| i.output_ref()) {
                self.process_consumed_txo(&ctx, &consumed, actions)?;
            }

            for (idx, produced) in tx.produces() {
                self.process_produced_txo(&tx, &produced, idx, actions)?;
            }
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self, policy: &crosscut::policies::RuntimePolicy) -> super::Reducer {
        let reducer = Reducer {
            config: self,
            policy: policy.clone(),
        };

        super::Reducer::UtxoByAddress(reducer)
    }
}
