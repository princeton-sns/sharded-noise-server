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

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SequencerRegisterReq {
    pub base_url: String,
    pub inbox_count: u8,
    pub outbox_count: u8,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SequencerRegisterResp {
    pub shard_id: u8,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SequencerShardMap {
    pub shards: Vec<String>,
}
