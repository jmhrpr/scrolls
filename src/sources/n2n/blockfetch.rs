use pallas::network::miniprotocols::{blockfetch, run_agent, Point, chainsync::Tip};

use gasket::{error::*, runtime::WorkOutcome};

use super::{transport::Transport, ChainSyncInternalPayload};
use crate::{crosscut, model::RawBlockPayload};

struct Observer<'a> {
    output: &'a mut OutputPort,
    chain_tip: Tip
}

impl<'a> blockfetch::Observer for Observer<'a> {
    fn on_block_received(&mut self, body: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        self.output.send(RawBlockPayload::roll_forward(body, self.chain_tip.clone()))?;

        Ok(())
    }
}

pub type InputPort = gasket::messaging::TwoPhaseInputPort<ChainSyncInternalPayload>;
pub type OutputPort = gasket::messaging::OutputPort<RawBlockPayload>;

pub struct Worker {
    address: String,
    input: InputPort,
    output: OutputPort,
    chain: crosscut::ChainWellKnownInfo,
    transport: Option<Transport>,
    block_count: gasket::metrics::Counter,
}

impl Worker {
    pub fn new(
        address: String,
        chain: crosscut::ChainWellKnownInfo,
        input: InputPort,
        output: OutputPort,
    ) -> Self {
        Self {
            address,
            chain,
            input,
            output,
            transport: None,
            block_count: Default::default(),
        }
    }

    fn fetch_block(&mut self, point: Point, chain_tip: Tip) -> Result<(), Error> {
        log::debug!("initiating chainsync");

        let observer = Observer {
            output: &mut self.output,
            chain_tip
        };

        let mut transport = self.transport.take().unwrap();

        let agent = blockfetch::BatchClient::initial((point.clone(), point), observer);
        run_agent(agent, &mut transport.channel3).or_restart()?;

        self.block_count.inc(1);
        self.transport = Some(transport);

        Ok(())
    }
}

impl gasket::runtime::Worker for Worker {
    fn metrics(&self) -> gasket::metrics::Registry {
        gasket::metrics::Builder::new()
            .with_counter("block_count", &self.block_count)
            .build()
    }

    fn bootstrap(&mut self) -> Result<(), Error> {
        let transport = Transport::setup(&self.address, self.chain.magic).or_retry()?;
        self.transport = Some(transport);

        Ok(())
    }

    fn work(&mut self) -> gasket::runtime::WorkResult {
        let input = self.input.recv_or_idle()?;

        match input.payload {
            ChainSyncInternalPayload::RollForward(point, chain_tip) => {
                self.fetch_block(point, chain_tip)?;
            }
            ChainSyncInternalPayload::RollBack(point) => {
                self.output.send(RawBlockPayload::roll_back(point))?;
            }
        };

        self.input.commit();
        Ok(WorkOutcome::Partial)
    }
}
