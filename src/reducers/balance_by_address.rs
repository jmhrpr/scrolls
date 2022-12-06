use pallas::ledger::traverse::MultiEraOutput;
use pallas::ledger::traverse::{MultiEraBlock, OutputRef};
use serde::Deserialize;

use crate::model::StorageAction;
use crate::{crosscut, model, prelude::*};

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub filter: Option<crosscut::filters::Predicate>,
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

        let address = utxo.address().map(|addr| addr.to_string()).or_panic()?;

        let key = match &self.config.key_prefix {
            Some(prefix) => format!("{}.{}", prefix, address),
            None => format!("{}.{}", "balance_by_address".to_string(), address),
        };

        let action = model::StorageAction::PNCounter(key, -1 * utxo.lovelace_amount() as i64);

        Ok(actions.push(action))
    }

    fn process_produced_txo(
        &mut self,
        tx_output: &MultiEraOutput,
        actions: &mut Vec<StorageAction>,
    ) -> Result<(), gasket::error::Error> {
        let address = tx_output.address().map(|x| x.to_string()).or_panic()?;

        let key = match &self.config.key_prefix {
            Some(prefix) => format!("{}.{}", prefix, address),
            None => format!("{}.{}", "balance_by_address".to_string(), address),
        };

        let action = model::StorageAction::PNCounter(key, tx_output.lovelace_amount() as i64);

        Ok(actions.push(action))
    }

    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        ctx: &model::BlockContext,
        actions: &mut Vec<StorageAction>,
    ) -> Result<(), gasket::error::Error> {
        for tx in block.txs().into_iter() {
            if filter_matches!(self, block, &tx, ctx) {
                for consumed in tx.consumes().iter().map(|i| i.output_ref()) {
                    self.process_consumed_txo(&ctx, &consumed, actions)?;
                }

                for (_, produced) in tx.produces() {
                    self.process_produced_txo(&produced, actions)?;
                }
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

        super::Reducer::BalanceByAddress(reducer)
    }
}
