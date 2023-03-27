use actix::{Actor, Context, Handler, Message};
use std::sync::Arc;

// #[derive(Message, Clone)]
// #[rtype(result = "()")]
// pub struct Initialize(pub Arc<crate::AppState>);

// pub struct SequencerActor {
//     epoch: u64,
//     outbox_signals: Vec<u16>,
//     state: Option<Arc<crate::AppState>>,
// }

// impl SequencerActor {
//     pub fn new() -> Self {
//         SequencerActor {
//             epoch: 1,
//             outbox_signals: Vec::new(),
//             state: None,
//         }
//     }
// }

// impl Actor for SequencerActor {
//     type Context = Context<Self>;
// }

// impl Handler<Initialize> for SequencerActor {
//     type Result = ();

//     fn handle(&mut self, msg: Initialize, _ctx: &mut Context<Self>) -> Self::Result {
//         self.state = Some(msg.0);

//         for inbox_actor in self.state.as_ref().unwrap().inbox_actors.iter() {
//             inbox_actor.do_send(crate::inbox::EpochStart(self.epoch));
//         }

//         // Timer::after(Duration::from_secs(1)).await;
//         // TODO: need to artificially create time between first two epochs
//         // this isn't good enough
//         // for i in 0..1000000 {
//         //     let x = i * 77 / 12;
//         // }

//         self.epoch += 1;
//         println!("Starting epoch {:?}", self.epoch);
//         for inbox_actor in self.state.as_ref().unwrap().inbox_actors.iter() {
//             inbox_actor.do_send(crate::inbox::EpochStart(self.epoch));
//         }
//     }
// }

// #[derive(Message, Clone)]
// #[rtype(result = "()")]
// pub struct EndEpoch(pub u64, pub u16);

// impl Handler<EndEpoch> for SequencerActor {
//     type Result = ();

//     fn handle(&mut self, msg: EndEpoch, _ctx: &mut Context<Self>) -> Self::Result {
//         if msg.0 == self.epoch - 1 {
//             let outbox_id = msg.1 as u16;
//             if !self.outbox_signals.contains(&outbox_id) {
//                 self.outbox_signals.push(outbox_id);
//             }

//             let num_outboxes = self.state.as_ref().unwrap().outbox_actors.len();
//             if self.outbox_signals.len() == num_outboxes {
//                 println!("ending epoch: {:?}", self.epoch);
//                 self.epoch += 1;
//                 println!("starting epoch: {:?}", self.epoch);
//                 self.outbox_signals = Vec::new();
//                 for inbox in self.state.as_ref().unwrap().inbox_actors.iter() {
//                     inbox.do_send(crate::inbox::EpochStart(self.epoch));
//                 }
//             }
//         }
//     }
// }
use serde::{Deserialize, Serialize};

#[derive(Message, Serialize, Deserialize, Clone, Debug)]
#[rtype(result = "SequencerRegisterResp")]
pub struct SequencerRegisterReq {
    pub base_url: String,
    pub inbox_count: u8,
    pub outbox_count: u8,
}

#[derive(MessageResponse, Serialize, Deserialize, Clone, Debug)]
pub struct SequencerRegisterResp {
    pub shard_id: u8,
}

#[derive(MessageResponse, Serialize, Deserialize, Clone, Debug)]
pub struct SequencerShardMapResp {
    pub shards: Vec<String>,
}

#[derive(Message, Serialize, Deserialize, Clone, Debug)]
#[rtype(result = "()")]
pub struct EndEpochReq {
    pub shard_id: u8,
    pub epoch_id: u64,
}

#[derive(Message, Clone, Debug)]
#[rtype(result = "SequencerShardMapResp")]
pub struct SequencerShardMapReq;

#[derive(Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct ProbeShards;

use actix::{Actor, Addr, Context, Handler, Message, MessageResponse};
use std::collections::{HashMap, LinkedList};
use std::mem;
use std::sync::Arc;

enum Phase {
    Registration,
    Bootup,
    Sequencing(u64),
}

#[derive(Message, Clone)]
#[rtype(result = "()")]
pub struct Initialize(pub Arc<super::ShardState>);

pub struct SequencerActor {
    num_shards: u8,
    shard_addresses: Vec<String>,
    phase: Phase,

}

impl SequencerActor {
    pub fn new(num_shards: u8) -> Self {
        SequencerActor {
            num_shards,
            shard_addresses: vec![],
            phase: Phase::Registration,
        }
    }
}

impl Actor for SequencerActor {
    type Context = Context<Self>;
}

impl Handler<SequencerRegisterReq> for SequencerActor {
    type Result = SequencerRegisterResp;
    
    fn handle(&mut self, msg: SequencerRegisterReq, ctx: Context<Self>) -> SequencerRegisterResp {
        self.shard_addresses.push_back(msg.base_url);

        if self.shard_addresses.len() == self.num_shards {
            ctx.notify_later(ProbeShards, duration::from_secs(1));
            self.phase = Phase::Bootup;
        }

        SequencerRegisterResp { shard_id: self.shard_addresses.len() - 1 }
    }
}

impl Handler<ProbeShards> for SequencerActor {
    type Result = ();

    fn handle(&mut self, _msg: ProbeShards, ctx: Context<Self>) {
    
        let adds = self.shard_addresses.clone();
        let this = ctx.address.clone()

        Box::pin(async move {

            let httpc = reqwest::Client();
            for addr in adds {
                if let Err(_) = httpc.get(addr).await {
                    tokio::sleep(tokio::Duration::from_sec(1)).await;
                    this.do_send(ProbeShards);
                    return
                }
            }

            this.do_send(EpochStart(0));
        })
    }

}


pub async fn init() -> impl Fn(&mut web::ServiceConfig) + Clone + Send + 'static {
    use std::io::{self, Write};

    Box::new(move |service_config: &mut web::ServiceConfig| {
        service_config
            .app_data(state.clone())
            // Client API
            .service(handle_message)
            .service(retrieve_messages)
            // Sequencer API
            .service(start_epoch)
            .service(index)
            // Intershard API
            .service(intershard_batch);
    })
}
