use pallas::{ledger::traverse::MultiEraBlock, network::miniprotocols::Point};

use crate::{
    crosscut,
    model::{self, StorageActionPayload},
    prelude::*,
    // rollback::buffer::RollbackBuffer,
    // model::StorageAction,
};

use super::Reducer;

type InputPort = gasket::messaging::TwoPhaseInputPort<model::EnrichedBlockPayload>;
type OutputPort = gasket::messaging::OutputPort<model::StorageActionPayload>;

// type ReducersResult = Vec<StorageAction>;

pub struct Worker {
    input: InputPort,
    output: OutputPort,
    reducers: Vec<Reducer>,
    // rollback_buffer: RollbackBuffer<ReducersResult>,
    policy: crosscut::policies::RuntimePolicy,
    ops_count: gasket::metrics::Counter,
    last_block: gasket::metrics::Gauge,
}

impl Worker {
    pub fn new(
        reducers: Vec<Reducer>,
        input: InputPort,
        output: OutputPort,
        policy: crosscut::policies::RuntimePolicy,
    ) -> Self {
        Worker {
            reducers,
            input,
            output,
            // rollback_buffer: Default::default(),
            policy,
            ops_count: Default::default(),
            last_block: Default::default(),
        }
    }

    fn reduce_block<'b>(
        &mut self,
        point: Point,
        block: &'b [u8],
        ctx: &model::BlockContext,
    ) -> Result<(), gasket::error::Error> {
        let block = MultiEraBlock::decode(block)
            .map_err(crate::Error::cbor)
            .apply_policy(&self.policy)
            .or_panic()?;

        let block = match block {
            Some(x) => x,
            None => return Ok(()),
        };

        self.last_block.set(block.number() as i64);

        let mut actions = Vec::new();

        // actions.push(StorageAction::block_starting(&block));

        // Instead of passing the output port to the reducers, we pass a vec which
        // we will add all the storage actions to, then we will send these down
        // the outport port later.
        for reducer in self.reducers.iter_mut() {
            reducer.reduce_block(&block, ctx, &mut actions)?;
            self.ops_count.inc(1);
        }

        // actions.push(StorageAction::block_finished(&block));

        // TODO merge related StorageActions in actions to reduce number of writes?

        // Push this block and it's stage result (storage actions) to the front
        // of the stage's rollback buffer
        // self.rollback_buffer
        //     .add_block(point.clone(), actions.clone());

        self.output.send(gasket::messaging::Message::from(
            StorageActionPayload::RollForward(point, actions),
        ))?;

        Ok(())
    }

    // TODO do we really need to store the storage actions here or can we just store them
    // in the storage stage? I.e, there is no rollback buffer in this stage.
    fn undo_blocks<'b>(&mut self, point: Point) -> Result<(), gasket::error::Error> {
        // fetch the rollbacked points and the associated storage actions from the
        // stage's rollback buffer
        // let points_and_results = match self.rollback_buffer.rollback_to_point(&point) {
        //     Ok(ps) => ps,
        //     Err(_) => panic!("unhandleable rollback"), // TODO
        // };

        // flatten all the storage actions executed by each rollbacked block.
        // this is sorted with most recently executed action to oldest (start:
        // last action of most recent block first, last: first action of oldest block).
        // let actions: Vec<StorageAction> = points_and_results
        //     .into_iter()
        //     .flat_map(|point| point.result.into_iter().rev())
        //     .collect();

        // send the storage actions down the pipeline so we can reverse the
        // changes they made to the storage
        // self.output.send(gasket::messaging::Message::from(
        //     StorageActionPayload::RollBack(point, actions),
        // ))?;

        self.output.send(gasket::messaging::Message::from(
            StorageActionPayload::RollBack(point),
        ))?;

        Ok(())
    }
}

impl gasket::runtime::Worker for Worker {
    fn metrics(&self) -> gasket::metrics::Registry {
        gasket::metrics::Builder::new()
            .with_counter("ops_count", &self.ops_count)
            .with_gauge("last_block", &self.last_block)
            .build()
    }

    fn work(&mut self) -> gasket::runtime::WorkResult {
        let msg = self.input.recv_or_idle()?;

        match msg.payload {
            model::EnrichedBlockPayload::RollForward(point, block, ctx) => {
                self.reduce_block(point, &block, &ctx)?
            }
            model::EnrichedBlockPayload::RollBack(point) => self.undo_blocks(point)?,
        }

        self.input.commit();
        Ok(gasket::runtime::WorkOutcome::Partial)
    }
}
