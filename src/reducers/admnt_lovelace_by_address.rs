/*

### ADAMINT REDUCER: LOVELACE BALANCE BY ADDRESS ###

KEY:    <prefix>.<base_address>

VALUE:  single value (lovelace)

CONFIG: list of bech32 addresses to monitor (hot wallet, cold wallet)

FILTER: only store balances for addresses in the filter

# Reducer Description

We want to monitor the ADA balance of the hot wallet so that we know when to move funds to
the cold wallet, and possibly the balance of the order payment addresses.

# Tests

[X] Balance increases correctly
[X] Balance decreases correctly
[X] Filter works correctly with single payment address

# Notes

We ignore changes in balance caused by invalid transactions which means our balance might
be less than in reality if we received any collateral return outputs, but this does not
matter.

We do not monitor multiassets, though multiassets are accounted for in the
AdmntUtxosByAddress reducer so that we can conserve value in txs.

*/

use pallas::ledger::addresses::Address;
use pallas::ledger::traverse::{MultiEraBlock, OutputRef};
use pallas::ledger::traverse::{MultiEraOutput, MultiEraTx};
use serde::Deserialize;

use crate::{crosscut, model, prelude::*};

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub payment_bech32: Vec<String>,
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
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let utxo = ctx.find_utxo(input).apply_policy(&self.policy).or_panic()?;

        let utxo = match utxo {
            Some(x) => x,
            None => return Ok(()),
        };

        let address = utxo.address().or_panic()?;

        // skip utxos for payment keys we are not monitoring
        if let Address::Shelley(ref a) = address {
            let pay_bech32 = a.payment().to_bech32().or_panic()?;
            if let Err(_) = &self.config.payment_bech32.binary_search(&pay_bech32) {
                return Ok(());
            }
        } else {
            return Ok(())
        }

        let address = address.to_bech32().or_panic()?;

        let key = match &self.config.key_prefix {
            Some(prefix) => format!("{}.{}", prefix, address),
            None => format!("{}.{}", "coin_by_address".to_string(), address),
        };

        let crdt = model::CRDTCommand::PNCounter(key, -1 * utxo.lovelace_amount() as i64);

        output.send(gasket::messaging::Message::from(crdt))?;

        Ok(())
    }

    fn process_outbound_txo(
        &mut self,
        tx_output: &MultiEraOutput,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let address = tx_output.address().or_panic()?;

        // skip utxos for payment keys we are not monitoring
        if let Address::Shelley(ref a) = address {
            let pay_bech32 = a.payment().to_bech32().or_panic()?;
            if let Err(_) = &self.config.payment_bech32.binary_search(&pay_bech32) {
                return Ok(());
            }
        } else {
            return Ok(())
        }

        let address = address.to_bech32().or_panic()?;

        let key = match &self.config.key_prefix {
            Some(prefix) => format!("{}.{}", prefix, address),
            None => format!("{}.{}", "coin_by_address".to_string(), address),
        };

        let crdt = model::CRDTCommand::PNCounter(key, tx_output.lovelace_amount() as i64);

        output.send(gasket::messaging::Message::from(crdt))?;

        Ok(())
    }

    pub fn reduce_valid_tx(
        &mut self,
        tx: &MultiEraTx,
        ctx: &model::BlockContext,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        for input in tx.inputs().iter().map(|i| i.output_ref()) {
            self.process_inbound_txo(&ctx, &input, output)?;
        }

        for (_idx, tx_output) in tx.outputs().iter().enumerate() {
            self.process_outbound_txo(tx_output, output)?;
        }

        Ok(())
    }

    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        ctx: &model::BlockContext,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        for tx in block.txs().into_iter() {
            if tx.is_valid() {
                self.reduce_valid_tx(&tx, ctx, output)?
            };
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

        super::Reducer::AdmntLovelaceByAddress(reducer)
    }
}
