/*

### ADAMINT REDUCER: MINT TX BY ASSET ###

KEY:    <prefix>.<asset_name_hex> OR <prefix>.<"royalty">

VALUE:  set of ("txhash,block_hash,block_height,block_slot")
  where
    txhash: hex string of transaction hash of tx which minted the asset
    block_*: details of block which contained mint tx

CONFIG: policyid: hex encoding of the PolicyID hash of the NFT collection being minted

FILTER: only store entries for multiassets with the configured policyid

# Reducer Description

We want to monitor if an NFT has been minted to mark an order as complete/settled or to
avoid a double mint in case of some failure in the minter. For example, if the NFT doesn't
exist on chain and X blocks have been mined since the mint tx TTL then we can safely
go about reminting the NFT.

# Tests

[X] correct information for named assets
[X] correct information for royalty token (unnamed)

# Notes

Only the asset name is stored in the key, not the policy, as all the entries in the
collection are assets of the configured policy ID.

The asset with the empty name is stored as the key "{prefix}.royalty", as it relates to
the CIP27 royalties standard.

Do we want to store the address which receives the asset? This means traversing the
minted assets and then the outputs and then crafting the CRDT commands.

*/

use gasket::error::AsWorkError;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraTx, MultiEraMint};
use pallas::crypto::hash::Hash;

use serde::Deserialize;

use crate::model;

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub nft_policy: String,
}

pub struct Reducer {
    config: Config,
}

impl Reducer {
    fn process_tx<'b>(
        &mut self,
        tx: &MultiEraTx,
        block: &'b MultiEraBlock<'b>,
        nft_policy: Hash<28>,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        if let MultiEraMint::AlonzoCompatible(multi_asset) = tx.mint() {
            for (policy_id, asset_names) in multi_asset.iter() {
                if *policy_id == nft_policy {
                    for asset in asset_names.iter() {
                        let mut asset_name = String::from(asset.0.clone());

                        // empty name token is cip27 royalties token
                        if asset_name.len() == 0 {
                            asset_name = String::from("royalty");
                        }

                        let value_str = format!("{},{},{},{}",
                            tx.hash(),
                            block.hash(),
                            block.number(),
                            block.slot()
                        );

                        // add to an ordered set, entries ordered by height
                        let crdt = model::CRDTCommand::any_write_wins(
                            self.config.key_prefix.as_deref(),
                            asset_name,
                            value_str,
                        );

                        output.send(crdt.into())?;
                    }
                }
            }
        }

        Ok(())
    }

    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let nft_policy = self.config.nft_policy.parse().or_panic()?;

        for tx in block.txs().into_iter() {
            if tx.is_valid() {
                self.process_tx(&tx, &block, nft_policy, output)?;
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

        super::Reducer::AdmntMintTxByAsset(reducer)
    }
}
