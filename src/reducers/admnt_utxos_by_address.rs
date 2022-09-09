/*

### ADAMINT REDUCER: UTXOS BY ADDRESS ###

KEY:    <prefix>.<base_address>

VALUE:  set ("txhash,idx,value_cbor")
  where
    txhash: tx which created the utxo (paired with idx identifies utxo)
    idx: txout index the utxo is at in the tx
    value_cbor: CBOR encoding of the UTXO value

CONFIG: [payment_bech32]: list of bech32 encoding of payment keys - we will monitor all
        addresses which use any of these payment keys.

FILTER: only store entries for addresses with a payment key in [payment_bech32]

# Reducer Description

We need to monitor the UTxOs currently held by the hot wallet to use as inputs in the
mint batcher and we need to monitor the UTxOs held by the order payment addresses so we
can move the funds to cold wallet or refund them.

# Tests

[X] Correct information ADA only
[X] Correct information with multiasset
[X] UTxOs creation
[X] UTxOs removal
[X] Correct address filtering

# Notes

We will only detect UTxOs created after the intersect. For that reason the
intersect must be set to some point before the initial funding of the hot wallet.

We do not monitor UTxOs in invalid transactions. The addresses we want to monitor
will not spend any UTxOs in invalid transactions. There is no reason why an honest
buyer would send us a collateral return output.

*/

use pallas::ledger::addresses::Address;
use pallas::ledger::traverse::MultiEraOutput;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraTx, OutputRef};
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

        let value_str = format!(
            "{},{},{}",
            input.hash(),
            input.index(),
            hex::encode(utxo.value().encode()),
        );

        let crdt = model::CRDTCommand::set_remove(
            self.config.key_prefix.as_deref(),
            &address.to_bech32().or_panic()?,
            value_str,
        );

        output.send(crdt.into())
    }

    fn process_outbound_txo(
        &mut self,
        tx: &MultiEraTx,
        tx_output: &MultiEraOutput,
        output_idx: usize,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let tx_hash = tx.hash();
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

        let value_str = format!(
            "{},{},{}",
            tx_hash,
            output_idx,
            hex::encode(tx_output.value().encode()),
        );

        let crdt = model::CRDTCommand::set_add(
            self.config.key_prefix.as_deref(),
            &address.to_bech32().or_panic()?,
            value_str,
        );

        output.send(crdt.into())
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

        for (idx, tx_output) in tx.outputs().iter().enumerate() {
            self.process_outbound_txo(tx, tx_output, idx, output)?;
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
            // we will not monitor invalid transactions
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

        super::Reducer::AdmntUtxosByAddress(reducer)
    }
}
