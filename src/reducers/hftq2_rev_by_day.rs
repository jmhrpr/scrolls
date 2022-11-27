use log::{debug, warn, error};
use pallas::ledger::traverse::{MultiEraOutput, MultiEraTx, ComputeHash};
use pallas::ledger::traverse::{MultiEraBlock, OutputRef};
use serde::Deserialize;

use crate::{crosscut, model, prelude::*};

/// we want to know all swap transactions, the ada made for each one and the slot in which is was mined

/// get total ada in inputs from our address
/// get total ada in outputs at our address
/// the difference is the revenue increase
/// add a key for the transaction
/// add the tx to a collection of all swap transactions

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: String,
    pub filter: Option<crosscut::filters::Predicate>,
    pub address: String,
}

pub struct Reducer {
    config: Config,
    policy: crosscut::policies::RuntimePolicy,
}

impl Reducer {
    fn process_inputs(
        &mut self,
        ctx: &model::BlockContext,
        target_address: &String,
        tx: &MultiEraTx,
    ) -> Result<u64, gasket::error::Error> {
        let mut total_spent = 0;

        // for every input, if the address is our address then add the value to the total spent
        for input in tx.consumes() {
            let utxo = ctx.find_utxo(&input.output_ref()).apply_policy(&self.policy).or_panic()?;

            let utxo = match utxo {
                Some(x) => x,
                None => {
                    debug!("couldn't find {:?} utxo in storage", input);

                    continue
                }
            };

            let address = utxo.address().map(|addr| addr.to_string()).or_panic()?;

            if address.eq(target_address) {
                total_spent += utxo.lovelace_amount();
            }
        }

        Ok(total_spent)
    }

    fn process_outputs(
        &mut self,
        target_address: &String,
        tx: &MultiEraTx,
    ) -> Result<u64, gasket::error::Error> {
        let mut total_received = 0;

        for (_, txout) in tx.produces() {
            let address = txout.address().map(|x| x.to_string()).or_panic()?;

            if address.eq(target_address) {
                total_received += txout.lovelace_amount()
            }
        }

        Ok(total_received)
    }

    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        ctx: &model::BlockContext,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        for tx in block.txs().into_iter() {
            if filter_matches!(self, block, &tx, ctx) {
                let spent = self.process_inputs(&ctx, &self.config.address.clone(), &tx)?;

                let received = self.process_outputs(&self.config.address.clone(), &tx)?;

                let revenue = match received.checked_sub(spent) {
                    Some(rev) => rev,
                    None => {
                        error!("revenue underflow");

                        0
                    }
                };

                warn!("possible match tx {} with rev {}", tx.hash(), revenue);

                if tx.plutus_v1_scripts().len() == 0 {
                    warn!("no scripts");

                    return Ok(())
                } else {
                    let hash = tx.plutus_v1_scripts().get(0).unwrap().compute_hash();
                    warn!("found script with hash: {hash}")
                }

                if revenue > 0 {
                    let val = format!("{},{},{}", block.slot(), tx.hash(), revenue);

                    let crdt = model::CRDTCommand::SetAdd("swaps".into(), val);

                    output.send(gasket::messaging::Message::from(crdt))?;
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

        super::Reducer::Hftq2RevByDay(reducer)
    }
}
