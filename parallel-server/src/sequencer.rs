use actix::{Actor, AsyncContext, Context, Handler, Message, MessageResponse, ResponseFuture};
use actix_web::web;
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

#[derive(Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct EpochStart(u64);

enum Phase {
    Registration,
    Bootup,
    Sequencing(u64),
}

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

    fn handle(
        &mut self,
        msg: SequencerRegisterReq,
        ctx: &mut Context<Self>,
    ) -> SequencerRegisterResp {
        self.shard_addresses.push(msg.base_url);

        if self.shard_addresses.len() == self.num_shards as usize {
            ctx.notify_later(ProbeShards, std::time::Duration::from_secs(1));
            self.phase = Phase::Bootup;
        }

        SequencerRegisterResp {
            shard_id: (self.shard_addresses.len() - 1) as u8,
        }
    }
}

impl Handler<ProbeShards> for SequencerActor {
    type Result = ResponseFuture<()>;

    fn handle(&mut self, _msg: ProbeShards, ctx: &mut Context<Self>) -> Self::Result {
        let adds = self.shard_addresses.clone();
        let this = ctx.address().clone();

        Box::pin(async move {
            let httpc = reqwest::Client::new();
            for addr in adds {
                if let Err(_) = httpc.get(addr).send().await {
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    this.do_send(ProbeShards);
                    return;
                }
            }

            this.do_send(EpochStart(0));
        })
    }
}

impl Handler<EpochStart> for SequencerActor {
    type Result = ResponseFuture<()>;

    fn handle(&mut self, msg: EpochStart, ctx: &mut Context<Self>) -> Self::Result {
        let EpochStart(epoch_id) = msg;
        self.phase = Phase::Sequencing(epoch_id);

        let _adds = self.shard_addresses.clone();
        let _this = ctx.address().clone();

        Box::pin(async move {
            // let httpc = reqwest::Client();
            // for addr in adds {
            //     if let Err(_) = httpc.get(addr).await {
            //         tokio::sleep(tokio::Duration::from_sec(1)).await;
            //         this.do_send(ProbeShards);
            //         return;
            //     }
            // }

            // this.do_send(EpochStart(0));
        })
    }
}

pub async fn init(num_shards: u8) -> impl Fn(&mut web::ServiceConfig) + Clone + Send + 'static {
    let sequencer_addr = SequencerActor::new(num_shards).start();

    Box::new(move |service_config: &mut web::ServiceConfig| {
        service_config.app_data(web::Data::new(sequencer_addr.clone()));
    })
}
