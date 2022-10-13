pub mod blockfetch;
pub mod chainsync;
mod transport;

use std::time::Duration;

use gasket::messaging::{OutputPort, TwoPhaseInputPort};

use pallas::network::miniprotocols::{Point, chainsync::Tip};
use serde::Deserialize;

use crate::{bootstrap, crosscut, model, storage};

#[derive(Clone, Debug)]
pub enum ChainSyncInternalPayload {
    RollForward(Point, Tip),
    RollBack(Point),
}

impl ChainSyncInternalPayload {
    pub fn roll_forward(point: Point, chain_tip: Tip) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::RollForward(point, chain_tip),
        }
    }

    pub fn roll_back(point: Point) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::RollBack(point),
        }
    }
}

#[derive(Deserialize)]
pub struct Config {
    pub address: String,
    pub min_depth: Option<usize>,
}

impl Config {
    pub fn bootstrapper(
        self,
        chain: &crosscut::ChainWellKnownInfo,
        intersect: &crosscut::IntersectConfig,
        finalize: &Option<crosscut::FinalizeConfig>,
    ) -> Bootstrapper {
        Bootstrapper {
            config: self,
            intersect: intersect.clone(),
            finalize: finalize.clone(),
            chain: chain.clone(),
            output: Default::default(),
        }
    }
}

pub struct Bootstrapper {
    config: Config,
    intersect: crosscut::IntersectConfig,
    finalize: Option<crosscut::FinalizeConfig>,
    chain: crosscut::ChainWellKnownInfo,
    output: OutputPort<model::RawBlockPayload>,
}

impl Bootstrapper {
    pub fn borrow_output_port(&mut self) -> &'_ mut OutputPort<model::RawBlockPayload> {
        &mut self.output
    }

    pub fn spawn_stages(self, pipeline: &mut bootstrap::Pipeline, cursor: storage::Cursor) {
        let mut headers_out = OutputPort::<ChainSyncInternalPayload>::default();
        let mut headers_in = TwoPhaseInputPort::<ChainSyncInternalPayload>::default();
        gasket::messaging::connect_ports(&mut headers_out, &mut headers_in, 10);

        pipeline.register_stage(gasket::runtime::spawn_stage(
            self::chainsync::Worker::new(
                self.config.address.clone(),
                self.config.min_depth.unwrap_or(0),
                self.chain.clone(),
                self.intersect,
                self.finalize,
                cursor,
                headers_out,
            ),
            gasket::runtime::Policy {
                tick_timeout: Some(Duration::from_secs(600)),
                bootstrap_retry: gasket::retries::Policy {
                    max_retries: 20,
                    backoff_factor: 2,
                    backoff_unit: Duration::from_secs(1),
                    max_backoff: Duration::from_secs(60),
                },
                ..Default::default()
            },
            Some("n2n-headers"),
        ));

        pipeline.register_stage(gasket::runtime::spawn_stage(
            self::blockfetch::Worker::new(
                self.config.address.clone(),
                self.chain,
                headers_in,
                self.output,
            ),
            gasket::runtime::Policy {
                tick_timeout: Some(Duration::from_secs(600)),
                bootstrap_retry: gasket::retries::Policy {
                    max_retries: 20,
                    backoff_factor: 2,
                    backoff_unit: Duration::from_secs(1),
                    max_backoff: Duration::from_secs(60),
                },
                ..Default::default()
            },
            Some("n2n-blocks"),
        ));
    }
}
