use pallas::ledger::traverse::MultiEraBlock;
use serde::Deserialize;

use crate::model::StorageAction;
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

impl Reducer {
    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        ctx: &model::BlockContext,
        actions: &mut Vec<StorageAction>,
    ) -> Result<(), gasket::error::Error> {
        if filter_matches_block!(self, block, ctx) {
            let value = block.header().cbor().to_vec();

            let action = StorageAction::any_write_wins(
                self.config.key_prefix.as_deref(),
                block.hash(),
                value,
            );

            actions.push(action)
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

        super::Reducer::BlockHeaderByHash(reducer)
    }
}
