use pallas::ledger::primitives::alonzo;
use pallas::ledger::primitives::alonzo::{PoolKeyhash, StakeCredential};
use pallas::ledger::traverse::MultiEraBlock;
use serde::Deserialize;

use crate::model::StorageAction;

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
}

pub struct Reducer {
    config: Config,
}

impl Reducer {
    fn key_value_write(
        &mut self,
        cred: &StakeCredential,
        pool: &PoolKeyhash,
        slot: u64,
        actions: &mut Vec<StorageAction>,
    ) -> Result<(), gasket::error::Error> {
        let key = match cred {
            StakeCredential::AddrKeyhash(x) => x.to_string(),
            StakeCredential::Scripthash(x) => x.to_string(),
        };

        let value = pool.to_string();

        let action = StorageAction::last_write_wins(
            self.config.key_prefix.as_deref(),
            &key,
            value,
            slot,
        );

        Ok(actions.push(action))
    }

    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        actions: &mut Vec<StorageAction>,
    ) -> Result<(), gasket::error::Error> {
        let slot = block.slot();

        for tx in block.txs() {
            if tx.is_valid() {
                for cert in tx.certs() {
                    if let Some(cert) = cert.as_alonzo() {
                        if let alonzo::Certificate::StakeDelegation(cred, pool) = cert {
                            self.key_value_write(cred, pool, slot, actions)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        let reducer = Reducer { config: self };
        super::Reducer::PoolByStake(reducer)
    }
}
