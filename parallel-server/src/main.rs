use actix::Addr;
use actix_web::{
    delete, error, get, http::header, post, web, App, HttpMessage, HttpServer, Responder,
};

#[get("/")]
async fn index() -> impl Responder {
    "Hello, World!"
}

#[get("/{name}")]
async fn hello(name: web::Path<String>) -> impl Responder {
    format!("Hello {}!", &name)
}

#[post("/epoch/{id}")]
async fn incr_epoch(id: web::Path<u64>, state: web::Data<AppState>) -> impl Responder {
    for inbox_actor in state.inbox_actors.iter() {
        inbox_actor.send(inbox::EpochStart(*id)).await.unwrap();
    }

    ""
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

fn hash_into_bucket(device_id: &str, bucket_count: usize) -> usize {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    device_id.hash(&mut hasher);
    (hasher.finish() % (bucket_count as u64)) as usize
}

#[post("/message")]
async fn handle_message(
    bundle: web::Json<protocol::Bundle>,
    state: web::Data<AppState>,
    auth: web::Header<BearerToken>,
) -> impl Responder {
    let sender_id = auth.into_inner().into_token();
    let inbox_actors_cnt = state.inbox_actors.len();
    let actor_idx = hash_into_bucket(&sender_id, inbox_actors_cnt);

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
    state: web::Data<AppState>,
    auth: web::Header<BearerToken>,
) -> impl Responder {
    let device_id = auth.into_inner().into_token();
    let outbox_actors_cnt = state.outbox_actors.len();
    let actor_idx = hash_into_bucket(&device_id, outbox_actors_cnt);

    let messages = state.outbox_actors[actor_idx]
        .1
        .send(outbox::GetDeviceMessages(device_id))
        .await
        .unwrap();

    web::Json(messages.0)
}

pub mod protocol {
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

    #[derive(Debug, Serialize, Clone)]
    #[serde(rename_all = "camelCase")]
    pub struct OutboxMessage {
        pub sender: String,
        pub receivers: Vec<String>,
        pub payload: Message,
        pub seq: (u64, u64),
    }
}

pub mod inbox {
    use crate::hash_into_bucket;
    use actix::{Actor, Addr, Context, Handler, Message};
    use std::collections::LinkedList;
    use std::sync::Arc;

    #[derive(Message, Clone)]
    #[rtype(result = "()")]
    pub struct Initialize(pub Arc<crate::AppState>);

    #[derive(Message, Debug, Clone)]
    #[rtype(result = "()")]
    pub struct EpochStart(pub u64);

    #[derive(Message, Debug, Clone)]
    #[rtype(result = "Result<u64, EventError>")]
    pub struct Event {
        pub sender: String,
        pub bundle: crate::protocol::Bundle,
    }

    #[derive(Debug)]
    pub enum EventError {
        NoEpoch,
    }

    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct InboxEpoch(pub Arc<crate::AppState>, pub u64, pub LinkedList<Event>);
    #[derive(Message, Clone, Debug)]
    #[rtype(result = "()")]
    pub struct RoutedEpochBatch {
        pub epoch_id: u64,
        pub inbox_id: u16,
        pub messages: LinkedList<crate::protocol::OutboxMessage>,
    }

    pub struct RouterActor {
        id: u16,
    }

    impl RouterActor {
        pub fn new(id: u16) -> Self {
            RouterActor { id }
        }
    }

    impl Actor for RouterActor {
        type Context = Context<Self>;
    }

    impl Handler<InboxEpoch> for RouterActor {
        type Result = ();

        fn handle(&mut self, msg: InboxEpoch, _ctx: &mut Context<Self>) {
            let InboxEpoch(state, epoch_id, queue) = msg;

            let mut outboxes = vec![LinkedList::new(); state.outbox_actors.len()];

            for (idx, ev) in queue.into_iter().enumerate() {
                let receivers: Vec<_> = ev
                    .bundle
                    .batch
                    .iter()
                    .map(|m| m.device_id.clone())
                    .collect();

                for message in ev.bundle.batch.into_iter() {
                    let bucket = hash_into_bucket(&message.device_id, outboxes.len());

                    let seq = (
                        epoch_id,
                        ((idx as u64) & 0x0000FFFFFFFFFFFF) | ((self.id as u64) << 48),
                    );

                    let rt_msg = crate::protocol::OutboxMessage {
                        sender: ev.sender.clone(),
                        receivers: receivers.clone(),
                        payload: message,
                        seq,
                    };

                    outboxes[bucket].push_back(rt_msg);
                }
            }

            for (messages, outbox) in outboxes.into_iter().zip(state.outbox_actors.iter()) {
                let routed_batch = RoutedEpochBatch {
                    epoch_id,
                    inbox_id: self.id,
                    messages,
                };

                outbox.0.do_send(routed_batch);
            }
        }
    }

    pub struct InboxActor {
        _id: u16,
        queue: LinkedList<Event>,
        pub epoch: Option<u64>,
        router: Addr<RouterActor>,
        state: Option<Arc<crate::AppState>>,
    }

    impl InboxActor {
        pub fn new(id: u16, router: Addr<RouterActor>) -> Self {
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
                    cur_queue,
                ));
            }

            self.epoch = Some(next_epoch);
        }
    }
}

pub mod outbox {
    use crate::inbox::RoutedEpochBatch;
    use actix::{Actor, Addr, Context, Handler, Message, MessageResponse};
    use std::collections::{HashMap, LinkedList};
    use std::mem;
    use std::sync::Arc;

    #[derive(Message, Clone)]
    #[rtype(result = "()")]
    pub struct Initialize(pub Arc<crate::AppState>);

    pub struct ReceiverActor {
        _id: u16,
        outbox_address: Addr<OutboxActor>,
        state: Option<Arc<crate::AppState>>,
        input_queues: Vec<Option<RoutedEpochBatch>>,
    }

    impl ReceiverActor {
        pub fn new(id: u16, outbox_address: Addr<OutboxActor>) -> Self {
            ReceiverActor {
                _id: id,
                outbox_address,
                state: None,
                input_queues: Vec::new(),
            }
        }
    }

    impl Actor for ReceiverActor {
        type Context = Context<Self>;
    }

    impl Handler<Initialize> for ReceiverActor {
        type Result = ();

        fn handle(&mut self, msg: Initialize, _ctx: &mut Context<Self>) -> Self::Result {
            self.input_queues = vec![None; msg.0.inbox_actors.len()];
            self.state = Some(msg.0);
        }
    }

    impl Handler<crate::inbox::RoutedEpochBatch> for ReceiverActor {
        type Result = ();

        fn handle(
            &mut self,
            msg: crate::inbox::RoutedEpochBatch,
            _ctx: &mut Context<Self>,
        ) -> Self::Result {
            let inbox_id = msg.inbox_id as usize;
            let epoch_id = msg.epoch_id as u64;
            self.input_queues[inbox_id] = Some(msg);

            // check if every element of input_queus has been written to
            if self.input_queues.iter().find(|v| v.is_none()).is_none() {
                let mut output_map = HashMap::new();
                for b in mem::replace(
                    &mut self.input_queues,
                    vec![None; self.state.as_ref().unwrap().inbox_actors.len()],
                )
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
        id: u16,
        sequencer: Addr<crate::sequencer::SequencerActor>,
        next_epoch: u64,
        // Mapping from device key to the next epoch which has not
        // been exposed to the client, and all messages from including
        // this message)
        client_mailboxes: HashMap<String, (u64, LinkedList<crate::protocol::OutboxMessage>)>,
    }

    impl OutboxActor {
        pub fn new(id: u16, sequencer: Addr<crate::sequencer::SequencerActor>) -> Self {
            OutboxActor {
                id,
                sequencer,
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
        pub HashMap<String, LinkedList<crate::protocol::OutboxMessage>>,
    );

    #[derive(MessageResponse)]
    pub struct DeviceMessages(pub LinkedList<crate::protocol::OutboxMessage>);

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
            self.sequencer
                .do_send(crate::sequencer::EndEpoch(epoch_id, self.id));
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

pub mod sequencer {
    use actix::{Actor, Context, Handler, Message};
    use std::sync::Arc;

    #[derive(Message, Clone)]
    #[rtype(result = "()")]
    pub struct Initialize(pub Arc<crate::AppState>);

    pub struct SequencerActor {
        epoch: u64,
        outbox_signals: Vec<u16>,
        state: Option<Arc<crate::AppState>>,
    }

    impl SequencerActor {
        pub fn new() -> Self {
            SequencerActor {
                epoch: 1,
                outbox_signals: Vec::new(),
                state: None,
            }
        }
    }

    impl Actor for SequencerActor {
        type Context = Context<Self>;
    }

    impl Handler<Initialize> for SequencerActor {
        type Result = ();

        fn handle(&mut self, msg: Initialize, _ctx: &mut Context<Self>) -> Self::Result {
            self.state = Some(msg.0);

            for inbox_actor in self.state.as_ref().unwrap().inbox_actors.iter() {
                inbox_actor.do_send(crate::inbox::EpochStart(self.epoch));
            }

            // Timer::after(Duration::from_secs(1)).await;
            // TODO: need to artificially create time between first two epochs
            // this isn't good enough
            // for i in 0..1000000 {
            //     let x = i * 77 / 12;
            // }

            self.epoch += 1;
            for inbox_actor in self.state.as_ref().unwrap().inbox_actors.iter() {
                inbox_actor.do_send(crate::inbox::EpochStart(self.epoch));
            }
        }
    }

    #[derive(Message, Clone)]
    #[rtype(result = "()")]
    pub struct EndEpoch(pub u64, pub u16);

    impl Handler<EndEpoch> for SequencerActor {
        type Result = ();

        fn handle(&mut self, msg: EndEpoch, _ctx: &mut Context<Self>) -> Self::Result {
            if msg.0 == self.epoch - 1 {
                let outbox_id = msg.1 as u16;
                if !self.outbox_signals.contains(&outbox_id) {
                    self.outbox_signals.push(outbox_id);
                }

                let num_outboxes = self.state.as_ref().unwrap().outbox_actors.len();
                if self.outbox_signals.len() == num_outboxes {
                    self.epoch += 1;
                    self.outbox_signals = Vec::new();
                    for inbox in self.state.as_ref().unwrap().inbox_actors.iter() {
                        inbox.do_send(crate::inbox::EpochStart(self.epoch));
                    }
                    println!("End epoch: {:?}", self.epoch);
                }
            }
        }
    }
}

const INBOX_ACTORS: u16 = 12;
const OUTBOX_ACTORS: u16 = 12;

pub struct AppState {
    inbox_actors: Vec<Addr<inbox::InboxActor>>,
    outbox_actors: Vec<(Addr<outbox::ReceiverActor>, Addr<outbox::OutboxActor>)>,
    _sequencer: Addr<sequencer::SequencerActor>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    use tokio::sync::mpsc;
    use actix::{Actor, Addr, Arbiter};

    let mut arbiters = Vec::new();

    let sequencer = sequencer::SequencerActor::new().start();

    // Boot up a set of inbox actors on individual arbiters:
    let mut inbox_actors: Vec<Addr<inbox::InboxActor>> = Vec::new();
    for id in 0..INBOX_ACTORS {
	let (router_tx, mut router_rx) = mpsc::channel(1);
	let router_arbiter = Arbiter::new();
	assert!(router_arbiter.spawn(async move {
	    let addr = inbox::RouterActor::new(id).start();
	    router_tx.send(addr).await;
	}));
	arbiters.push(router_arbiter);
	let router = router_rx.recv().await.unwrap();

	let (inbox_tx, mut inbox_rx) = mpsc::channel(1);
	let inbox_arbiter = Arbiter::new();
	assert!(inbox_arbiter.spawn(async move {
	    let addr = inbox::InboxActor::new(id, router).start();
	    inbox_tx.send(addr).await;
	}));
	arbiters.push(inbox_arbiter);

	inbox_actors.push(inbox_rx.recv().await.unwrap());
    }

    // Boot up a set of outbox actors on individual arbiters:
    let mut outbox_actors: Vec<(Addr<outbox::ReceiverActor>, Addr<outbox::OutboxActor>)> = Vec::new();
    for id in 0..OUTBOX_ACTORS {
	let (outbox_tx, mut outbox_rx) = mpsc::channel(1);
	let outbox_arbiter = Arbiter::new();
	let sequencer_clone = sequencer.clone();
	assert!(outbox_arbiter.spawn(async move {
	    let addr = outbox::OutboxActor::new(id, sequencer_clone).start();
	    outbox_tx.send(addr).await;
	}));
	arbiters.push(outbox_arbiter);
	let outbox_addr = outbox_rx.recv().await.unwrap();

	let (recv_tx, mut recv_rx) = mpsc::channel(1);
	let recv_arbiter = Arbiter::new();
	let outbox_addr_clone = outbox_addr.clone();
	assert!(recv_arbiter.spawn(async move {
	    let addr = outbox::ReceiverActor::new(id, outbox_addr_clone).start();
	    recv_tx.send(addr).await;
	}));
	arbiters.push(recv_arbiter);

	outbox_actors.push((recv_rx.recv().await.unwrap(), outbox_addr));
    }


    // // Boot up a set of outbox actors:
    // let outbox_actors: Vec<(Addr<outbox::ReceiverActor>, Addr<outbox::OutboxActor>)> = (0
    //     ..OUTBOX_ACTORS)
    //     .map(|id| {
    //         let out_id = outbox::OutboxActor::new(id, sequencer.clone()).start();
    //         let rec_id = outbox::ReceiverActor::new(id, out_id.clone()).start();
    //         (rec_id, out_id)
    //     })
    //     .collect();

    let state = web::Data::new(AppState {
        _sequencer: sequencer.clone(),
        inbox_actors,
        outbox_actors,
    });

    for inbox_actor in state.inbox_actors.iter() {
        inbox_actor
            .send(inbox::Initialize(state.clone().into_inner()))
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

    sequencer
        .send(sequencer::Initialize(state.clone().into_inner()))
        .await
        .unwrap();

    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .service(index)
            .service(hello)
            .service(handle_message)
            .service(retrieve_messages)
            .service(incr_epoch)
    })
    .bind(("127.0.0.1", 8081))?
    .run()
    .await
}

