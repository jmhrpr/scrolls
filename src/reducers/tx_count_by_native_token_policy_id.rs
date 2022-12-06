use serde::Deserialize;

use pallas::ledger::traverse::{Feature, MultiEraBlock};

use crate::crosscut;
use crate::crosscut::epochs::block_epoch;
use crate::model::StorageAction;

#[derive(Deserialize, Copy, Clone)]
pub enum AggrType {
    Epoch,
}

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub aggr_by: Option<AggrType>,
}

pub struct Reducer {
    config: Config,
    chain: crosscut::ChainWellKnownInfo,
}

impl Reducer {
    fn config_key(&self, policy_id: String, epoch_no: u64) -> String {
        let def_key_prefix = "transaction_count_by_native_token_policy";

        match &self.config.aggr_by {
            Some(aggr_type) => match aggr_type {
                AggrType::Epoch => {
                    return match &self.config.key_prefix {
                        Some(prefix) => format!("{}.{}.{}", prefix, policy_id, epoch_no),
                        None => format!("{}.{}", def_key_prefix.to_string(), policy_id),
                    };
                }
            },
            None => {
                return match &self.config.key_prefix {
                    Some(prefix) => format!("{}.{}", prefix, policy_id),
                    None => format!("{}.{}", def_key_prefix.to_string(), policy_id),
                };
            }
        };
    }

    pub fn reduce_block(
        &mut self,
        block: &MultiEraBlock,
        actions: &mut Vec<StorageAction>,
    ) -> Result<(), gasket::error::Error> {
        if block.era().has_feature(Feature::MultiAssets) {
            let epoch_no = block_epoch(&self.chain, block);

            for tx in block.txs() {
                if tx.is_valid() {
                    let mint = tx.mint();

                    if let Some(mints) = mint.as_alonzo() {
                        for (policy, assets) in mints.iter() {
                            let policy_id = hex::encode(policy.as_slice());

                            let number_of_minted_or_destroyed = assets.len();

                            let key = self.config_key(policy_id, epoch_no);

                            let action =
                                StorageAction::PNCounter(key, number_of_minted_or_destroyed as i64);

                            actions.push(action)
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self, chain: &crosscut::ChainWellKnownInfo) -> super::Reducer {
        let reducer = Reducer {
            config: self,
            chain: chain.clone(),
        };

        super::Reducer::TxCountByNativeTokenPolicyId(reducer)
    }
}
