use std::time::Duration;

use gasket::{
    error::AsWorkError,
    runtime::{spawn_stage, WorkOutcome},
};

use log::warn;
use pallas::{
    codec::minicbor,
    ledger::traverse::{Era, MultiEraBlock, MultiEraTx, OutputRef},
    network::miniprotocols::Point,
};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde::Deserialize;
use sled::IVec;

use crate::{
    bootstrap, crosscut,
    model::{self, BlockContext},
    prelude::AppliesPolicy,
    rollback::buffer::{RollbackBuffer, RollbackResult},
};

type InputPort = gasket::messaging::TwoPhaseInputPort<model::RawBlockPayload>;
type OutputPort = gasket::messaging::OutputPort<model::EnrichedBlockPayload>;

#[derive(Deserialize, Clone)]
pub struct Config {
    pub db_path: String,
}

impl Config {
    pub fn boostrapper(self, policy: &crosscut::policies::RuntimePolicy) -> Bootstrapper {
        Bootstrapper {
            config: self,
            policy: policy.clone(),
            input: Default::default(),
            output: Default::default(),
        }
    }
}

pub struct Bootstrapper {
    config: Config,
    policy: crosscut::policies::RuntimePolicy,
    input: InputPort,
    output: OutputPort,
}

impl Bootstrapper {
    pub fn borrow_input_port(&mut self) -> &'_ mut InputPort {
        &mut self.input
    }

    pub fn borrow_output_port(&mut self) -> &'_ mut OutputPort {
        &mut self.output
    }

    pub fn spawn_stages(self, pipeline: &mut bootstrap::Pipeline) {
        let worker = Worker {
            config: self.config,
            policy: self.policy,
            db: None,
            input: self.input,
            output: self.output,
            rollback_buffer: Default::default(),
            inserts_counter: Default::default(),
            remove_counter: Default::default(),
            matches_counter: Default::default(),
            mismatches_counter: Default::default(),
            blocks_counter: Default::default(),
        };

        pipeline.register_stage(spawn_stage(
            worker,
            gasket::runtime::Policy {
                tick_timeout: Some(Duration::from_secs(600)),
                ..Default::default()
            },
            Some("enrich-sled"),
        ));
    }
}

pub struct Worker {
    config: Config,
    policy: crosscut::policies::RuntimePolicy,
    db: Option<sled::Db>,
    input: InputPort,
    output: OutputPort,
    rollback_buffer: RollbackBuffer<EnrichResult>,
    inserts_counter: gasket::metrics::Counter,
    remove_counter: gasket::metrics::Counter,
    matches_counter: gasket::metrics::Counter,
    mismatches_counter: gasket::metrics::Counter,
    blocks_counter: gasket::metrics::Counter,
}

#[derive(Clone)]
struct SledTxValue(u16, Vec<u8>);

impl TryInto<IVec> for SledTxValue {
    type Error = crate::Error;

    fn try_into(self) -> Result<IVec, Self::Error> {
        let SledTxValue(era, body) = self;
        minicbor::to_vec((era, body))
            .map(|x| IVec::from(x))
            .map_err(crate::Error::cbor)
    }
}

impl TryFrom<IVec> for SledTxValue {
    type Error = crate::Error;

    fn try_from(value: IVec) -> Result<Self, Self::Error> {
        let (tag, body): (u16, Vec<u8>) = minicbor::decode(&value).map_err(crate::Error::cbor)?;

        Ok(SledTxValue(tag, body))
    }
}

/// A UTxO consumed by the Tx and therefore removed from the db, with its value so
/// that we have the information required to re-add the UTxO on rollback.
type ConsumedUtxo = (String, SledTxValue);
/// A UTxO produced by the Tx and therefore added to the db. We don't need the
/// value because to undo this operation we just need to delete the key (TxId).
type ProducedUtxo = String;
/// The cumulitive results of the enrich stage: the list of consumed UTxOs and
/// produced UTxOs as a result of processing the block.
type EnrichResult = (Vec<ConsumedUtxo>, Vec<ProducedUtxo>);

#[inline]
fn fetch_referenced_utxo<'a>(
    db: &sled::Db,
    utxo_ref: &OutputRef,
) -> Result<Option<(OutputRef, Era, Vec<u8>)>, crate::Error> {
    if let Some(ivec) = db
        .get(utxo_ref.to_string())
        .map_err(crate::Error::storage)?
    {
        let SledTxValue(era, cbor) = ivec.try_into().map_err(crate::Error::storage)?;
        let era: Era = era.try_into().map_err(crate::Error::storage)?;
        Ok(Some((utxo_ref.clone(), era, cbor)))
    } else {
        Ok(None)
    }
}

impl Worker {
    #[inline]
    fn insert_produced_utxos(
        &self,
        db: &sled::Db,
        txs: &[MultiEraTx],
    ) -> Result<Vec<ProducedUtxo>, crate::Error> {
        let mut insert_batch = sled::Batch::default();
        let mut produced = Vec::new();

        for tx in txs.iter() {
            for (idx, output) in tx.produces() {
                let txout_id = format!("{}#{}", tx.hash(), idx);

                let key: IVec = txout_id.as_bytes().into();

                let era = tx.era().into();
                let body = output.encode();
                let value: IVec = SledTxValue(era, body).try_into()?;

                insert_batch.insert(key, value);
                produced.push(txout_id);
            }
        }

        db.apply_batch(insert_batch)
            .map_err(crate::Error::storage)?;

        self.inserts_counter.inc(txs.len() as u64);

        Ok(produced)
    }

    #[inline]
    fn par_fetch_referenced_utxos(
        &self,
        db: &sled::Db,
        txs: &[MultiEraTx],
    ) -> Result<BlockContext, crate::Error> {
        let mut ctx = BlockContext::default();

        let required: Vec<_> = txs
            .iter()
            .flat_map(|tx| tx.requires())
            .map(|input| input.output_ref())
            .collect();

        let matches: Result<Vec<_>, crate::Error> = required
            .par_iter()
            .map(|utxo_ref| fetch_referenced_utxo(db, utxo_ref))
            .collect();

        for m in matches? {
            if let Some((key, era, cbor)) = m {
                ctx.import_ref_output(&key, era, cbor);
                self.matches_counter.inc(1);
            } else {
                self.mismatches_counter.inc(1);
            }
        }

        Ok(ctx)
    }

    fn remove_consumed_utxos(
        &self,
        db: &sled::Db,
        txs: &[MultiEraTx],
    ) -> Result<Vec<ConsumedUtxo>, crate::Error> {
        let mut consumed = Vec::new();

        let keys: Vec<_> = txs
            .iter()
            .flat_map(|tx| tx.consumes())
            .map(|i| i.output_ref())
            .collect();

        for key in keys.iter() {
            // this returns the value of the deleted key (if present)
            let ret = db
                .remove(key.to_string().as_bytes())
                .map_err(crate::Error::storage)?;

            match ret {
                Some(value) => consumed.push((
                    key.to_string(),
                    value.try_into().map_err(crate::Error::storage)?,
                )),
                None => warn!("tried to remove UTxO from enrich db which didn't exist: {key}"),
            }
        }

        self.remove_counter.inc(keys.len() as u64);

        Ok(consumed)
    }
}

impl gasket::runtime::Worker for Worker {
    fn metrics(&self) -> gasket::metrics::Registry {
        gasket::metrics::Builder::new()
            .with_counter("enrich_inserts", &self.inserts_counter)
            .with_counter("enrich_removes", &self.remove_counter)
            .with_counter("enrich_matches", &self.matches_counter)
            .with_counter("enrich_mismatches", &self.mismatches_counter)
            .with_counter("enrich_blocks", &self.blocks_counter)
            .build()
    }

    fn work(&mut self) -> gasket::runtime::WorkResult {
        let msg = self.input.recv_or_idle()?;

        match msg.payload {
            model::RawBlockPayload::RollForward(cbor) => {
                let block = MultiEraBlock::decode(&cbor)
                    .map_err(crate::Error::cbor)
                    .apply_policy(&self.policy)
                    .or_panic()?;

                let block = match block {
                    Some(x) => x,
                    None => return Ok(gasket::runtime::WorkOutcome::Partial),
                };

                let point = Point::Specific(block.slot(), block.hash().to_vec());

                let db = self.db.as_ref().unwrap();

                let txs = block.txs();

                // first we insert new utxo produced in this block
                let produced = self.insert_produced_utxos(db, &txs).or_restart()?;

                // then we fetch referenced utxo in this block
                let ctx = self.par_fetch_referenced_utxos(db, &txs).or_restart()?;

                // and finally we remove utxos consumed by the block
                let consumed = self.remove_consumed_utxos(db, &txs).or_restart()?;

                // then we collect information about which utxos were added to
                // or removed from the db for this block and add it to the
                // rollback buffer
                let enrich_effects = (consumed, produced);

                self.rollback_buffer.add_block(point, enrich_effects);

                // then we send the block down the pipeline
                self.output
                    .send(model::EnrichedBlockPayload::roll_forward(cbor, ctx))?;

                self.blocks_counter.inc(1);
            }
            model::RawBlockPayload::RollBack(rb_point) => {
                // for all the blocks which succeeded the rollback point, retrieve the consumed/produced utxos
                let points_and_results = match self.rollback_buffer.rollback_to_point(&rb_point) {
                    RollbackResult::PointFound(ps) => ps,
                    RollbackResult::PointNotFound => panic!(), // TODO
                };

                let consumed: Vec<ConsumedUtxo> = points_and_results
                    .iter()
                    .map(|p| p.result.clone())
                    .flat_map(|(c, _)| c)
                    .collect();

                let produced: Vec<ProducedUtxo> = points_and_results
                    .iter()
                    .map(|p| p.result.clone())
                    .flat_map(|(_, p)| p)
                    .collect();

                // let to_undo: Vec<Point> = points_and_results.into_iter().map(|p| p.point).collect(); TODO

                let db = self.db.as_ref().unwrap();

                let mut batch = sled::Batch::default();

                // TODO check this logic:

                // we want to avoid situation where we are inserting utxos which
                // where consumed in the blocks but were also produced earlier
                // in the rollbacked blocks. we reinsert all the utxos which were
                // consumed by txs in the blocks, but this might include utxos
                // which were also produced by txs in the blocks, so then we
                // remove all the utxos which were produced by the blocks.

                // re-insert all the UTxOs that the rollback blocks consumed
                for utxo in consumed {
                    let key: IVec = utxo.0.as_bytes().into();
                    let value: IVec = utxo.1.try_into().or_restart()?;

                    batch.insert(key, value)
                }

                // remove all the UTxOs that the rollbacked blocks produced
                for utxo in produced {
                    let key: IVec = utxo.as_bytes().into();

                    batch.remove(key)
                }

                db.apply_batch(batch)
                    .map_err(crate::Error::storage)
                    .or_restart()?;

                self.output
                    .send(model::EnrichedBlockPayload::roll_back(rb_point))?;
            }
        };

        self.input.commit();
        Ok(WorkOutcome::Partial)
    }

    fn bootstrap(&mut self) -> Result<(), gasket::error::Error> {
        let db = sled::open(&self.config.db_path).or_retry()?;
        self.db = Some(db);

        Ok(())
    }

    fn teardown(&mut self) -> Result<(), gasket::error::Error> {
        match &self.db {
            Some(db) => {
                db.flush().or_panic()?;
                Ok(())
            }
            None => Ok(()),
        }
    }
}
