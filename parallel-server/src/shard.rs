use actix::{Actor, Addr};
use actix_web::{
    delete, error, get, http::header, post, web, App, HttpMessage, HttpResponse, HttpServer,
    Responder,
};
use std::io;
use tokio::time::{sleep, Duration};

pub mod client_protocol {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone)]
    #[serde(rename_all = "camelCase")]
    pub struct Payload {
        pub c_type: usize,
        pub ciphertext: String,
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    #[serde(rename_all = "camelCase")]
    pub struct Message {
        pub device_id: String,
        pub payload: Payload,
    }

    #[derive(Debug, Deserialize, Clone)]
    #[serde(rename_all = "camelCase")]
    pub struct Bundle {
        pub batch: Vec<Message>,
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    #[serde(rename_all = "camelCase")]
    pub struct OutboxMessage {
        pub sender: String,
        pub receivers: Vec<String>,
        pub payload: Message,
        pub seq: (u64, u64),
    }
}

pub mod inbox {
    use super::hash_into_bucket;
    use actix::{Actor, Addr, Context, Handler, Message};
    use std::collections::LinkedList;
    use std::sync::Arc;

    #[derive(Message, Clone)]
    #[rtype(result = "()")]
    pub struct Initialize(pub Arc<super::ShardState>);

    #[derive(Message, Debug, Clone)]
    #[rtype(result = "()")]
    pub struct EpochStart(pub u64);

    #[derive(Message, Debug, Clone)]
    #[rtype(result = "Result<u64, EventError>")]
    pub struct Event {
        pub sender: String,
        pub bundle: super::client_protocol::Bundle,
    }

    #[derive(Debug)]
    pub enum EventError {
        NoEpoch,
    }

    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct InboxEpoch(
        pub Arc<super::ShardState>,
        pub u64,
        pub u8,
        pub LinkedList<Event>,
    );
    #[derive(Message, Clone, Debug)]
    #[rtype(result = "()")]
    pub struct RoutedEpochBatch {
        pub epoch_id: u64,
        pub inbox_id: u8,
        pub messages: LinkedList<super::client_protocol::OutboxMessage>,
    }

    struct RouterActor {
        id: u8,
    }

    impl RouterActor {
        pub fn new(id: u8) -> Self {
            RouterActor { id }
        }
    }

    impl Actor for RouterActor {
        type Context = Context<Self>;
    }

    impl Handler<InboxEpoch> for RouterActor {
        type Result = ();

        fn handle(&mut self, msg: InboxEpoch, _ctx: &mut Context<Self>) {
            let InboxEpoch(state, epoch_id, shard_id, queue) = msg;

            let mut intershard = vec![LinkedList::new(); state.intershard_router_actors.len()];

            for (idx, ev) in queue.into_iter().enumerate() {
                let receivers: Vec<_> = ev
                    .bundle
                    .batch
                    .iter()
                    .map(|m| m.device_id.clone())
                    .collect();

                for message in ev.bundle.batch.into_iter() {
                    let bucket = hash_into_bucket(&message.device_id, intershard.len(), true);

                    let seq = (
                        epoch_id,
                        ((idx as u64) & 0x0000FFFFFFFFFFFF)
                            | ((self.id as u64) << 48)
                            | ((shard_id as u64) << 56),
                    );

                    let rt_msg = super::client_protocol::OutboxMessage {
                        sender: ev.sender.clone(),
                        receivers: receivers.clone(),
                        payload: message,
                        seq,
                    };

                    intershard[bucket].push_back(rt_msg);
                }
            }

            for (messages, intershard_rt) in intershard
                .into_iter()
                .zip(state.intershard_router_actors.iter())
            {
                let routed_batch = RoutedEpochBatch {
                    epoch_id,
                    inbox_id: self.id,
                    messages,
                };

                intershard_rt.do_send(routed_batch);
            }
        }
    }

    pub struct InboxActor {
        _id: u8,
        queue: LinkedList<Event>,
        epoch: Option<u64>,
        router: Addr<RouterActor>,
        state: Option<Arc<super::ShardState>>,
    }

    impl InboxActor {
        pub fn new(id: u8) -> Self {
            let router = RouterActor::new(id).start();

            InboxActor {
                _id: id,
                queue: LinkedList::new(),
                epoch: None,
                router,
                state: None,
            }
        }
    }

    impl Actor for InboxActor {
        type Context = Context<Self>;
    }

    impl Handler<Initialize> for InboxActor {
        type Result = ();

        fn handle(&mut self, msg: Initialize, _ctx: &mut Context<Self>) -> Self::Result {
            self.state = Some(msg.0);
        }
    }

    impl Handler<Event> for InboxActor {
        type Result = Result<u64, EventError>;

        fn handle(&mut self, ev: Event, _ctx: &mut Context<Self>) -> Self::Result {
            if let Some(ref epoch_id) = self.epoch {
                // Simply add the event to the queue:
                self.queue.push_back(ev);
                Ok(*epoch_id)
            } else {
                Err(EventError::NoEpoch)
            }
        }
    }

    impl Handler<EpochStart> for InboxActor {
        type Result = ();

        fn handle(&mut self, sequencer_msg: EpochStart, _ctx: &mut Context<Self>) -> Self::Result {
            use std::mem;

            let EpochStart(next_epoch) = sequencer_msg;

            // If we've already started an epoch, ensure that this is
            // the subsequent epoch and move the current queue
            // contents to the router.
            if let Some(ref cur_epoch) = self.epoch {
                assert!(*cur_epoch + 1 == next_epoch);

                // Swap out the queue and send it to the router:
                let cur_queue = mem::replace(&mut self.queue, LinkedList::new());
                self.router.do_send(InboxEpoch(
                    self.state.as_ref().unwrap().clone(),
                    *cur_epoch,
                    self.state.as_ref().unwrap().shard_id,
                    cur_queue,
                ));
            }

            self.epoch = Some(next_epoch);
        }
    }
}

pub mod intershard {
    use actix::{Actor, Addr, Context, Handler, Message, MessageResponse, ResponseFuture};
    use serde::{Deserialize, Serialize};
    use std::collections::{HashMap, LinkedList};
    use std::mem;
    use std::sync::Arc;

    #[derive(Message, Serialize, Deserialize, Clone, Debug)]
    #[rtype(result = "()")]
    pub struct RoutedEpochBatch {
        pub src_shard_id: u8,
        pub dst_shard_id: u8,
        pub epoch_id: u64,
        pub messages: LinkedList<super::client_protocol::OutboxMessage>,
    }

    #[derive(Message, Clone)]
    #[rtype(result = "()")]
    pub struct Initialize(pub Arc<super::ShardState>);

    pub struct InterShardRouterActor {
        src_shard_id: u8,
        dst_shard_id: u8,
        state: Option<Arc<super::ShardState>>,
        input_queues: (usize, Vec<Option<super::inbox::RoutedEpochBatch>>),
    }

    impl InterShardRouterActor {
        pub fn new(src_shard_id: u8, dst_shard_id: u8) -> Self {
            InterShardRouterActor {
                src_shard_id,
                dst_shard_id,
                state: None,
                input_queues: (0, vec![]),
            }
        }
    }

    impl Actor for InterShardRouterActor {
        type Context = Context<Self>;
    }

    impl Handler<Initialize> for InterShardRouterActor {
        type Result = ();

        fn handle(&mut self, msg: Initialize, _ctx: &mut Context<Self>) -> Self::Result {
            let Initialize(state) = msg;
            self.input_queues = (0, vec![None; state.inbox_actors.len()]);
            self.state = Some(state);
        }
    }

    impl Handler<super::inbox::RoutedEpochBatch> for InterShardRouterActor {
        type Result = ResponseFuture<()>;

        fn handle(
            &mut self,
            msg: super::inbox::RoutedEpochBatch,
            _ctx: &mut Context<Self>,
        ) -> Self::Result {
            let inbox_id = msg.inbox_id as usize;
            let epoch_id = msg.epoch_id as u64;
            assert!(self.input_queues.1[inbox_id].is_none());
            self.input_queues.1[inbox_id] = Some(msg);
            self.input_queues.0 += 1;

            // check if every element of input_queus has been written to
            if self.input_queues.0 == self.input_queues.1.len() {
                let mut shard_batch = LinkedList::new();

                for queue in self.input_queues.1.iter_mut() {
                    shard_batch.append(&mut queue.as_mut().unwrap().messages);
                }

                self.input_queues = (
                    0,
                    vec![None; self.state.as_ref().unwrap().inbox_actors.len()],
                );

                if self.src_shard_id == self.dst_shard_id {
                    distribute_intershard_batch(
                        self.state.as_ref().unwrap(),
                        RoutedEpochBatch {
                            src_shard_id: self.src_shard_id,
                            dst_shard_id: self.dst_shard_id,
                            epoch_id: epoch_id,
                            messages: shard_batch,
                        },
                    );
                    Box::pin(async move {})
                } else {
                    let dst_shard_url =
                        self.state.as_ref().unwrap().shard_map[self.dst_shard_id as usize].clone();
                    let (src_shard_id, dst_shard_id) = (self.src_shard_id, self.dst_shard_id);
                    let httpc = self.state.as_ref().unwrap().httpc.clone();

                    Box::pin(async move {
                        httpc
                            .post(format!("{}/intershard-batch", dst_shard_url))
                            .json(&RoutedEpochBatch {
                                src_shard_id: src_shard_id,
                                dst_shard_id: dst_shard_id,
                                epoch_id: epoch_id,
                                messages: shard_batch,
                            })
                            .send()
                            .await
                            .unwrap();
                    })
                }
            } else {
                Box::pin(async move {})
            }
        }
    }

    pub fn distribute_intershard_batch(state: &super::ShardState, batch: RoutedEpochBatch) {
        assert!(batch.dst_shard_id == state.shard_id);

        let mut outboxes = vec![LinkedList::new(); state.outbox_actors.len()];

        for message in batch.messages.into_iter() {
            let bucket = super::hash_into_bucket(&message.payload.device_id, outboxes.len(), false);
            outboxes[bucket].push_back(message);
        }

        for (messages, outbox) in outboxes.into_iter().zip(state.outbox_actors.iter()) {
            let routed_batch = RoutedEpochBatch {
                epoch_id: batch.epoch_id,
                src_shard_id: batch.src_shard_id,
                dst_shard_id: state.shard_id,
                messages,
            };

            outbox.0.do_send(routed_batch);
        }
    }
}

pub mod outbox {
    use actix::{Actor, Addr, Context, Handler, Message, MessageResponse};
    use std::collections::{HashMap, LinkedList};
    use std::mem;
    use std::sync::Arc;

    #[derive(Message, Clone)]
    #[rtype(result = "()")]
    pub struct Initialize(pub Arc<super::ShardState>);

    pub struct ReceiverActor {
        _id: u8,
        outbox_address: Addr<OutboxActor>,
        state: Option<Arc<super::ShardState>>,
        input_queues: (usize, Vec<Option<super::intershard::RoutedEpochBatch>>),
    }

    impl ReceiverActor {
        pub fn new(id: u8, outbox_address: Addr<OutboxActor>) -> Self {
            ReceiverActor {
                _id: id,
                outbox_address,
                state: None,
                input_queues: (0, Vec::new()),
            }
        }
    }

    impl Actor for ReceiverActor {
        type Context = Context<Self>;
    }

    impl Handler<Initialize> for ReceiverActor {
        type Result = ();

        fn handle(&mut self, msg: Initialize, _ctx: &mut Context<Self>) -> Self::Result {
            self.input_queues = (0, vec![None; msg.0.shard_map.len()]);
            self.state = Some(msg.0);
        }
    }

    impl Handler<super::intershard::RoutedEpochBatch> for ReceiverActor {
        type Result = ();

        fn handle(
            &mut self,
            msg: super::intershard::RoutedEpochBatch,
            _ctx: &mut Context<Self>,
        ) -> Self::Result {
            let shard_id = msg.src_shard_id as usize;
            let epoch_id = msg.epoch_id as u64;
            assert!(self.input_queues.1[shard_id].is_none());
            self.input_queues.1[shard_id] = Some(msg);
            self.input_queues.0 += 1;

            // check if every element of input_queus has been written to
            if self.input_queues.0 == self.input_queues.1.len() {
                let mut output_map = HashMap::new();
                for b in mem::replace(
                    &mut self.input_queues,
                    (0, vec![None; self.state.as_ref().unwrap().shard_map.len()]),
                )
                .1
                .into_iter()
                {
                    let batch = b.unwrap();
                    for message in batch.messages.into_iter() {
                        let device_messages = output_map
                            .entry(message.payload.device_id.clone())
                            .or_insert_with(|| LinkedList::new());
                        device_messages.push_back(message);
                    }
                }
                self.outbox_address
                    .do_send(DeviceEpochBatch(epoch_id, output_map))
            }
        }
    }

    pub struct OutboxActor {
        id: u8,
        next_epoch: u64,
        // Mapping from device key to the next epoch which has not
        // been exposed to the client, and all messages from including
        // this message)
        client_mailboxes: HashMap<String, (u64, LinkedList<super::client_protocol::OutboxMessage>)>,
    }

    impl OutboxActor {
        pub fn new(id: u8) -> Self {
            OutboxActor {
                id,
                next_epoch: 0,
                client_mailboxes: HashMap::new(),
            }
        }
    }

    impl Actor for OutboxActor {
        type Context = Context<Self>;
    }

    #[derive(Message, Clone, Debug)]
    #[rtype(result = "()")]
    pub struct DeviceEpochBatch(
        pub u64,
        pub HashMap<String, LinkedList<super::client_protocol::OutboxMessage>>,
    );

    #[derive(MessageResponse)]
    pub struct DeviceMessages(pub LinkedList<super::client_protocol::OutboxMessage>);

    #[derive(Message, Clone, Debug)]
    #[rtype(result = "DeviceMessages")]
    pub struct GetDeviceMessages(pub String);

    impl Handler<DeviceEpochBatch> for OutboxActor {
        type Result = ();

        fn handle(
            &mut self,
            epoch_batch: DeviceEpochBatch,
            _ctx: &mut Context<Self>,
        ) -> Self::Result {
            let DeviceEpochBatch(epoch_id, device_messages) = epoch_batch;

            for (device, mut messages) in device_messages.into_iter() {
                if let Some((_, ref mut device_mailbox)) = self.client_mailboxes.get_mut(&device) {
                    device_mailbox.append(&mut messages);
                } else {
                    self.client_mailboxes.insert(device, (0, messages));
                }
            }

            self.next_epoch = epoch_id + 1;

            unimplemented!()
            // self.sequencer
            //     .do_send(crate::sequencer::EndEpoch(epoch_id, self.id));
        }
    }

    impl Handler<GetDeviceMessages> for OutboxActor {
        type Result = DeviceMessages;

        fn handle(&mut self, msg: GetDeviceMessages, _ctx: &mut Context<Self>) -> Self::Result {
            let (ref mut client_next_epoch, ref mut client_msgs) = self
                .client_mailboxes
                .entry(msg.0)
                .or_insert_with(|| (0, LinkedList::new()));

            *client_next_epoch = self.next_epoch;

            let msgs = mem::replace(client_msgs, LinkedList::new());
            DeviceMessages(msgs)
        }
    }
}

pub struct BearerToken(String);

impl BearerToken {
    pub fn token(&self) -> &str {
        &self.0
    }

    pub fn into_token(self) -> String {
        self.0
    }
}

impl header::TryIntoHeaderValue for BearerToken {
    type Error = header::InvalidHeaderValue;

    fn try_into_value(self) -> Result<header::HeaderValue, Self::Error> {
        header::HeaderValue::from_str(&format!("Bearer {}", self.0))
    }
}

impl header::Header for BearerToken {
    fn name() -> header::HeaderName {
        header::AUTHORIZATION
    }

    fn parse<M: HttpMessage>(msg: &M) -> Result<Self, error::ParseError> {
        msg.headers()
            .get(Self::name())
            .ok_or(error::ParseError::Header)
            .and_then(|h| h.to_str().map_err(|_| error::ParseError::Header))
            .and_then(|h| h.strip_prefix("Bearer ").ok_or(error::ParseError::Header))
            .map(|t| BearerToken(t.to_string()))
    }
}

fn hash_into_bucket(device_id: &str, bucket_count: usize, upper_bits: bool) -> usize {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    device_id.hash(&mut hasher);
    let hash = if upper_bits {
        let [a, b, c, d, _, _, _, _] = u64::to_be_bytes(hasher.finish());
        u32::from_be_bytes([a, b, c, d])
    } else {
        let [_, _, _, _, a, b, c, d] = u64::to_be_bytes(hasher.finish());
        u32::from_be_bytes([a, b, c, d])
    };

    (hash % (bucket_count as u32)) as usize
}

#[get("/")]
async fn index() -> impl Responder {
    HttpResponse::NoContent().finish()
}

#[post("/message")]
async fn handle_message(
    bundle: web::Json<client_protocol::Bundle>,
    state: web::Data<ShardState>,
    auth: web::Header<BearerToken>,
) -> impl Responder {
    let sender_id = auth.into_inner().into_token();
    let inbox_actors_cnt = state.inbox_actors.len();
    let actor_idx = hash_into_bucket(&sender_id, inbox_actors_cnt, false);

    // Now, send the message to the corresponding actor:
    let epoch = state.inbox_actors[actor_idx]
        .send(inbox::Event {
            sender: sender_id,
            bundle: bundle.into_inner(),
        })
        .await
        .unwrap()
        .unwrap();

    web::Json::<u64>(epoch)
}

#[delete("/outbox")]
async fn retrieve_messages(
    state: web::Data<ShardState>,
    auth: web::Header<BearerToken>,
) -> impl Responder {
    let device_id = auth.into_inner().into_token();
    let outbox_actors_cnt = state.outbox_actors.len();
    let actor_idx = hash_into_bucket(&device_id, outbox_actors_cnt, false);

    let messages = state.outbox_actors[actor_idx]
        .1
        .send(outbox::GetDeviceMessages(device_id))
        .await
        .unwrap();

    web::Json(messages.0)
}

#[post("/epoch/{epoch_id}")]
async fn start_epoch(state: web::Data<ShardState>, epoch_id: web::Path<u64>) -> impl Responder {
    for inbox in state.inbox_actors.iter() {
        inbox.send(inbox::EpochStart(*epoch_id));
    }
    ""
}

#[post("/intershard-batch")]
async fn intershard_batch(
    state: web::Data<ShardState>,
    batch: web::Json<intershard::RoutedEpochBatch>,
) -> impl Responder {
    intershard::distribute_intershard_batch(&*state, batch.into_inner());
    ""
}

pub struct ShardState {
    httpc: reqwest::Client,
    shard_id: u8,
    shard_map: Vec<String>,
    inbox_actors: Vec<Addr<inbox::InboxActor>>,
    intershard_router_actors: Vec<Addr<intershard::InterShardRouterActor>>,
    outbox_actors: Vec<(Addr<outbox::ReceiverActor>, Addr<outbox::OutboxActor>)>,
}

pub async fn init(
    shard_base_url: String,
    sequencer_base_url: String,
    inbox_count: u8,
    outbox_count: u8,
) -> impl Fn(&mut web::ServiceConfig) + Clone + Send + 'static {
    use std::io::{self, Write};

    let httpc = reqwest::Client::new();

    // Register ourselves with the sequencer to be assigned a shard id
    println!(
        "Registering ourselves ({}) at sequencer ({}) with {}/{} inboxes/outboxes...",
        shard_base_url, sequencer_base_url, inbox_count, outbox_count
    );
    let register_resp = httpc
        .post(format!("{}/register", &sequencer_base_url))
        .json(&crate::sequencer::SequencerRegisterReq {
            base_url: shard_base_url,
            inbox_count,
            outbox_count,
        })
        .send()
        .await
        .unwrap()
        .json::<crate::sequencer::SequencerRegisterResp>()
        .await
        .unwrap();
    println!(
        "Successfully registered, have been assigned shard id {}",
        register_resp.shard_id
    );

    // Now, wait until all shards have been registered at the sequencer and get
    // their IDs:
    let mut shard_map_opt: Option<crate::sequencer::SequencerShardMap> = None;
    println!("Trying to retrieve shard map, this will loop until the expected number of shards have registered...");
    while shard_map_opt.is_none() {
        let resp = httpc
            .get(format!("{}/shardmap", &sequencer_base_url))
            .send()
            .await;

        if let Ok(res) = resp {
            shard_map_opt = Some(
                res.json::<crate::sequencer::SequencerShardMap>()
                    .await
                    .unwrap(),
            );
        } else {
            print!(".");
            io::stdout().flush();
            sleep(Duration::from_millis(500));
        }
    }
    println!("Retrieved shard map!");
    let shard_map = shard_map_opt.unwrap();

    // Boot up a set of inbox actors:
    let inbox_actors: Vec<Addr<inbox::InboxActor>> = (0..inbox_count)
        .map(|id| inbox::InboxActor::new(id).start())
        .collect();

    // Boot up a set of intershard routers:
    let intershard_router_actors: Vec<Addr<intershard::InterShardRouterActor>> = (0..shard_map
        .shards
        .len())
        .map(|id| intershard::InterShardRouterActor::new(register_resp.shard_id, id as u8).start())
        .collect();

    // Boot up a set of outbox actors:
    let outbox_actors: Vec<(Addr<outbox::ReceiverActor>, Addr<outbox::OutboxActor>)> = (0
        ..outbox_count)
        .map(|id| {
            let out_id = outbox::OutboxActor::new(id).start();
            let rec_id = outbox::ReceiverActor::new(id, out_id.clone()).start();
            (rec_id, out_id)
        })
        .collect();

    let state = web::Data::new(ShardState {
        httpc,
        shard_id: register_resp.shard_id,
        shard_map: shard_map.shards,
        inbox_actors,
        intershard_router_actors,
        outbox_actors,
    });

    for inbox_actor in state.inbox_actors.iter() {
        inbox_actor
            .send(inbox::Initialize(state.clone().into_inner()))
            .await
            .unwrap();
    }

    for intershard_router_actor in state.intershard_router_actors.iter() {
        intershard_router_actor
            .send(intershard::Initialize(state.clone().into_inner()))
            .await
            .unwrap();
    }

    for outbox_actor in state.outbox_actors.iter() {
        outbox_actor
            .0
            .send(outbox::Initialize(state.clone().into_inner()))
            .await
            .unwrap();
    }

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
