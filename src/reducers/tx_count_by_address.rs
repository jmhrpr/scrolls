use pallas::ledger::traverse::MultiEraOutput;
use pallas::ledger::traverse::{MultiEraBlock, OutputRef};
use serde::Deserialize;
use std::collections::HashSet;

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
    fn process_inbound_txo(
        &mut self,
        ctx: &model::BlockContext,
        input: &OutputRef,
        seen: &mut HashSet<String>,
        actions: &mut Vec<StorageAction>,
    ) -> Result<(), gasket::error::Error> {
        let utxo = ctx.find_utxo(input).apply_policy(&self.policy).or_panic()?;

        let utxo = match utxo {
            Some(x) => x,
            None => return Ok(()),
        };

        let address = utxo.address().map(|addr| addr.to_string()).or_panic()?;

        if seen.insert(address.clone()) {
            let key = match &self.config.key_prefix {
                Some(prefix) => format!("{}.{}", prefix, address),
                None => format!("{}.{}", "txcount_by_address".to_string(), address),
            };

            let action = StorageAction::PNCounter(key, 1);

            actions.push(action)
        }

        Ok(())
    }

    fn process_outbound_txo(
        &mut self,
        tx_output: &MultiEraOutput,
        seen: &mut HashSet<String>,
        actions: &mut Vec<StorageAction>,
    ) -> Result<(), gasket::error::Error> {
        let address = tx_output.address().map(|x| x.to_string()).or_panic()?;

        if seen.insert(address.clone()) {
            let key = match &self.config.key_prefix {
                Some(prefix) => format!("{}.{}", prefix, address),
                None => format!("{}.{}", "txcount_by_address".to_string(), address),
            };

            let action = model::StorageAction::PNCounter(key, 1);

            actions.push(action)
        }

        Ok(())
    }

    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        ctx: &model::BlockContext,
        actions: &mut Vec<StorageAction>,
    ) -> Result<(), gasket::error::Error> {
        for tx in block.txs().into_iter() {
            if filter_matches!(self, block, &tx, ctx) {
                let mut seen = HashSet::new();

                for input in tx.inputs().iter().map(|i| i.output_ref()) {
                    self.process_inbound_txo(&ctx, &input, &mut seen, actions)?;
                }

                for (_idx, tx_output) in tx.outputs().iter().enumerate() {
                    self.process_outbound_txo(tx_output, &mut seen, actions)?;
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

        super::Reducer::TxCountByAddress(reducer)
    }
}
