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
    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        actions: &mut Vec<StorageAction>,
    ) -> Result<(), gasket::error::Error> {
        let block_hash = block.hash();
        let block_slot = block.slot();

        for tx in &block.txs() {
            let key = match &self.config.key_prefix {
                Some(prefix) => format!("{}.{}", prefix, tx.hash()),
                None => format!("{}", tx.hash()),
            };

            let member = format!("{},{}", block_slot, block_hash);

            let action = StorageAction::SetAdd(key, member);

            actions.push(action);
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        let worker = Reducer { config: self };
        super::Reducer::PointByTx(worker)
    }
}
