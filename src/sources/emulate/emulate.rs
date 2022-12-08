use log::debug;
use pallas::ledger::traverse::MultiEraBlock;
use pallas::network::miniprotocols::Point;
use std::fs;
use std::{thread, time};

use crate::model;

type OutputPort = gasket::messaging::OutputPort<model::RawBlockPayload>;

enum Instruction {
    Forwards,
    Backwards(u8),
}

struct EmulatorInstructions(Vec<Instruction>);

impl TryFrom<String> for EmulatorInstructions {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let mut ops = Vec::new();
        let mut tokens = value.chars();

        loop {
            let token = match tokens.next() {
                Some(t) => t,
                None => return Ok(EmulatorInstructions(ops)),
            };

            match token {
                'F' => ops.push(Instruction::Forwards),
                'B' => {
                    let amount_token = match tokens.next() {
                        Some(t) => t,
                        None => {
                            return Err(
                                "Backwards instruction B must be immediately followed by a u8",
                            )
                        }
                    };

                    let amount: u8 = match amount_token.to_digit(10) {
                        Some(n) => n as u8,
                        None => return Err("Invalid backwards instruction amount"),
                    };

                    if amount == 0 {
                        return Err("Backwards amount cannot be 0");
                    }

                    ops.push(Instruction::Backwards(amount))
                }
                _ => {
                    return Err(
                        "Invalid emulator instruction, must be 'F' or 'Bn' where 'n' is a u8",
                    );
                }
            }
        }
    }
}

pub struct Worker {
    instructions: EmulatorInstructions,
    blocks: Vec<Vec<u8>>,
    output: OutputPort,
    block_count: gasket::metrics::Counter,
    chain_tip: gasket::metrics::Gauge,
}

impl Worker {
    pub fn new(instructions: Option<String>, output: OutputPort) -> Self {
        let ops = EmulatorInstructions::try_from(instructions.unwrap_or("FFF".into())).unwrap();

        Self {
            instructions: ops,
            output,
            block_count: Default::default(),
            chain_tip: Default::default(),
            blocks: Vec::new(),
        }
    }
}

impl gasket::runtime::Worker for Worker {
    fn metrics(&self) -> gasket::metrics::Registry {
        gasket::metrics::Builder::new()
            .with_counter("received_blocks", &self.block_count)
            .with_gauge("chain_tip", &self.chain_tip)
            .build()
    }

    /// Read all of the test block-bytes files into a vector
    fn bootstrap(&mut self) -> Result<(), gasket::error::Error> {
        let mut blocks_bytes = Vec::new();

        for path in fs::read_dir("../../src/sources/emulate/block_data/").unwrap() {
            let path = path.unwrap();

            if path.file_type().unwrap().is_file() {
                let contents = hex::decode(fs::read(path.path()).unwrap()).unwrap();

                blocks_bytes.push(contents);
            }
        }

        self.blocks = blocks_bytes;

        Ok(())
    }

    fn work(&mut self) -> gasket::runtime::WorkResult {
        let mut block_ptr = 0;

        for op in self.instructions.0.iter() {
            match op {
                Instruction::Forwards => {
                    let block_bytes = self.blocks.get(block_ptr).unwrap();

                    let block = MultiEraBlock::decode(&block_bytes).unwrap();

                    let point = Point::Specific(block.slot(), block.hash().to_vec());

                    block_ptr += 1;

                    debug!("emulating rollforward to {:?}", point);

                    self.output.send(model::RawBlockPayload::roll_forward(
                        point.clone(),
                        block_bytes.to_vec(),
                    ))?;
                }
                Instruction::Backwards(n) => {
                    debug!("block_ptr = {block_ptr}, n = {}", *n);

                    let block_bytes = self.blocks.get(block_ptr - (*n as usize + 1)).unwrap();

                    let block = MultiEraBlock::decode(&block_bytes).unwrap();

                    let point = Point::Specific(block.slot(), block.hash().to_vec());

                    debug!("emulating rollbackwards to {:?}", point);

                    self.output
                        .send(model::RawBlockPayload::roll_back(point.clone()))?;
                }
            }

            thread::sleep(time::Duration::from_secs(20));
        }

        Ok(gasket::runtime::WorkOutcome::Done)
    }
}
