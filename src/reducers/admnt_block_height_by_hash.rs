/*

### ADAMINT REDUCER: BLOCK HEIGHT BY BLOCK HASH ###

KEY:    <prefix>.<block_hash>

VALUE:  single value (block_height, block_slot)

FILTER: only store blocks which contain txs that send an order payment address; that is an
        address which uses the configurable payment key. and potentially minting the NFTs?

# Reducer Description

We want to be able to check if a block no longer exists (rollbacked) or how many
confirmations the block has, for example when monitoring the confirmations of a
payment tx or NFT mint.

# Tests

[X] filters block correctly
[X] correct information

# Notes

Do we really need this or can we just use block heights stored in other collections?

TODO we need to remove rollbacked blocks

*/

use pallas::ledger::traverse::MultiEraBlock;
use serde::de::value;
use serde::{Deserialize, Serialize};

use crate::prelude::*;
use crate::{crosscut, model};

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub filter: Option<crosscut::filters::Predicate>,
}

pub struct Reducer {
    config: Config,
    policy: crosscut::policies::RuntimePolicy,
}

#[derive(Serialize)]
pub struct AdmntBlockHeight {
    height: u64,
    slot: u64,
}

impl Reducer {
    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        ctx: &model::BlockContext,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        if filter_matches_block!(self, block, ctx) {
            let value_str = serde_json::to_string(&AdmntBlockHeight {
                height: block.number(),
                slot: block.slot()
            }).or_panic()?;

            let crdt = model::CRDTCommand::any_write_wins(
                self.config.key_prefix.as_deref(),
                block.hash(),
                value_str,
            );

            output.send(gasket::messaging::Message::from(crdt))?;
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

        super::Reducer::AdmntBlockHeightByHash(reducer)
    }
}
