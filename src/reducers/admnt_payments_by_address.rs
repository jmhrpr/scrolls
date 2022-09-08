/*

### ADAMINT REDUCER: PAYMENTS BY ADDRESS ###

KEY:    <prefix>.<base_address>

VALUE:  sorted set ("txhash,lovelace,credit,block_hash,block_height,block_slot", height)
  where
    txhash: hex string of transaction hash of tx containing the payment/the payment tx
    lovelace: total amount of lovelace received by base_address in the tx
    credit: 'T' if credit token configured AND at least 1 received in tx, 'F' otherwise
    block_hash: block hash of block which contained the tx

CONFIG: payment_vkh: bech32 encoding of the payment key used in all payment addresses
        free_mint_token_policy: optional PolicyID hash (hex) of a free-mint credit token
        which buyers can use instead of paying mint_price etc/whitelist redemption

FILTER: only store entries for addresses with payment key payment_vkh

# Reducer Description

We want to track payments to all order payment addresses, where only the payment part of
the address is known beforehand. A payment is a single transaction which sends the
payment address some funds and the payment amount is the total value received by the
payment address in the transaction (so accounts for multiple TxOuts). For each payment we
need to know...

    txhash: so we can mark an accepted payment so as to not refund it (depending on dzn)
    lovelace: so we can check if at least mint_price was received
    credit: so we know if the buyer used a "free-mint" token in the payment
    block_hash: so we can check the block has not rollbacked when checking for confs
    block_height: so we can immediately check how many confirmations the payment has
    block_slot: so we can check if the payment was received before the order expiry slot

# Tests

[X] correct ADA amount received in single txout (+ with other recipients present)
[X] correct ADA amount received in multiple txouts
[X] correct detection of free mint credit token in payment amount
[X] correct filtering of full address from payment key bech32 in config

# Notes

We only process valid transactions. This means we can trust the TxOuts, but we won't
detect payments which /technically/ could be made using CollRet.

The credit flag is set to 'T' (true) if at least one of /any/ token with the supplied
free_mint_token_policy is received by the base_address in the tx, regardless of asset
name.

*/

use std::collections::BTreeMap;

use pallas::ledger::addresses::Address;
use pallas::ledger::primitives::babbage::Value;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraTx};
use pallas::crypto::hash::Hash;

use serde::Deserialize;

use crate::{model, prelude::*};

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub payment_vkh: String,
    pub free_mint_token_policy: Option<String>,
}

pub struct Reducer {
    config: Config,
}

impl Reducer {
    fn process_tx<'b>(
        &mut self,
        tx: &MultiEraTx,
        block: &'b MultiEraBlock<'b>,
        mint_credit_policy: Option<Hash<28>>,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {

        // create map of address -> (ada_received, mint_credit_received)
        let mut addr_map: BTreeMap<String, (u64, bool)> = BTreeMap::new();

        for (_idx, tx_output) in tx.outputs().iter().enumerate() {
            if let Address::Shelley(addr) = tx_output.address().or_panic()? {
                let txo_addr_payment = addr.payment().to_bech32().or_panic()?;

                if txo_addr_payment == self.config.payment_vkh {
                    let entry = addr_map.entry(addr.to_bech32().or_panic()?)
                        .or_insert((0, false));

                    entry.0 += tx_output.ada_amount();

                    // flag if any output in tx contains a free-mint credit
                    if let Some(credit_policy) = mint_credit_policy {
                        entry.1 |= value_contains_free_mint_token(
                            credit_policy,
                            tx_output.value()
                        ).or_panic()?
                    }
                }
            }
        }

        // send CRDT messages for all order payment addresses found tx
        for (address, &(amount, credit)) in addr_map.iter() {
            let value_str = format!("{},{},{},{},{},{}",
                tx.hash(),
                amount,
                if credit {"T"} else {"F"},
                block.hash(),
                block.number(),
                block.slot()
            );

            // add to an ordered set, entries ordered by height
            let crdt = model::CRDTCommand::last_write_wins(
                self.config.key_prefix.as_deref(),
                address,
                value_str,
                block.number(),
            );

            output.send(crdt.into())?;
        }

        Ok(())
    }

    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {

        let maybe_credit_policy = match &self.config.free_mint_token_policy {
            Some(policy_hex) => Some(policy_hex.parse().or_panic()?),
            None => None
        };

        for tx in block.txs().into_iter() {
            if tx.is_valid() {
                self.process_tx(&tx, &block, maybe_credit_policy, output)?;
            }
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        let reducer = Reducer {
            config: self,
        };

        super::Reducer::AdmntPaymentsByAddress(reducer)
    }
}

// Return true iff the value contains at least 1 of /any/ token with the given policy,
// regardless of asset name. We check the amount as well as the presence of the policy
// because it may be possible to include '0' amount of an asset in the tx serialisation.
fn value_contains_free_mint_token(
    policy: Hash<28>, value: Value
) -> Result<bool, gasket::error::Error> {
    if let Value::Multiasset(_, ma) = value {
        if let Some(assets) = ma.get(&policy) {
            if let Some((_asset, amount)) = assets.iter().next() {
                if *amount >= 1 {return Ok(true)};
            }
        }
    }

    Ok(false)
}