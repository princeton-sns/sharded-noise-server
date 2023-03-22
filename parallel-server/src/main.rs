use actix::Addr;

use actix_web::{error, get, http::header, post, web, App, HttpMessage, HttpServer, Responder};
use std::sync::Arc;

#[get("/")]
async fn index() -> impl Responder {
    "Hello, World!"
}

#[get("/{name}")]
async fn hello(name: web::Path<String>) -> impl Responder {
    format!("Hello {}!", &name)
}

#[get("/epoch/{id}")]
async fn incr_epoch(id: web::Path<u64>, state: web::Data<AppState>) -> impl Responder {

    for inbox_actor in state.inbox_actors.iter() {
        inbox_actor.send(inbox::EpochStart(*id)).await.unwrap();
    }

    ""
}

struct BearerToken(String);

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
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let inbox_actors_cnt = state.inbox_actors.len();

    // Hash the sender_id to determine the corresponding inbox actor
    let sender_id = auth.into_inner().into_token();
    let mut sender_hasher = DefaultHasher::new();
    sender_id.hash(&mut sender_hasher);
    let actor_idx = sender_hasher.finish() % (inbox_actors_cnt as u64);

    // Now, send the message to the corresponding actor:
    let epoch = state.inbox_actors[actor_idx as usize]
        .send(inbox::Event {
            sender: sender_id,
            bundle: bundle.into_inner(),
        })
        .await
        .unwrap()
        .unwrap();

    web::Json::<u64>(epoch)
}

pub mod protocol {
    use serde::Deserialize;

    #[derive(Debug, Deserialize, Clone)]
    #[serde(rename_all = "camelCase")]
    pub struct Payload {
        pub c_type: usize,
        pub ciphertext: String,
    }

    #[derive(Debug, Deserialize, Clone)]
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

    #[derive(Debug, Clone)]
    pub struct RoutedMessage {
        pub sender: String,
        pub payload: crate::protocol::Message,
        pub inbox_index: u64,
    }

    #[derive(Message, Clone, Debug)]
    #[rtype(result = "()")]
    pub struct RoutedEpochBatch {
        pub epoch_id: u64,
        pub inbox_id: u16,
        pub messages: LinkedList<RoutedMessage>,
    }

    struct RouterActor {
        id: u16,
        epoch: Option<InboxEpoch>,
    }

    impl RouterActor {
        pub fn new(id: u16) -> Self {
            RouterActor { id, epoch: None }
        }
    }

    impl Actor for RouterActor {
        type Context = Context<Self>;
    }

    impl Handler<InboxEpoch> for RouterActor {
        type Result = ();

        fn handle(&mut self, msg: InboxEpoch, _ctx: &mut Context<Self>) {
            let InboxEpoch(state, epoch_id, queue) = msg;

            let mut mailboxes = vec![LinkedList::new(); state.outbox_actors.len()];

            for (idx, ev) in queue.into_iter().enumerate() {
                for message in ev.bundle.batch.into_iter() {
                    let bucket = hash_into_bucket(&message.device_id, mailboxes.len());
                    let rt_msg = RoutedMessage {
                        sender: ev.sender.clone(),
                        payload: message,
                        inbox_index: idx as u64,
                    };

                    mailboxes[bucket].push_back(rt_msg);
                }
            }

            for (messages, outbox) in mailboxes.into_iter().zip(state.outbox_actors.iter()) {
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
        id: u16,
        queue: LinkedList<Event>,
        epoch: Option<u64>,
        router: Addr<RouterActor>,
        state: Option<Arc<crate::AppState>>,
    }

    impl InboxActor {
        pub fn new(id: u16) -> Self {
            let router = RouterActor::new(id).start();

            InboxActor {
                id,
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
    use crate::inbox::{RoutedEpochBatch, RoutedMessage};
    use actix::{Actor, Addr, Context, Handler, Message};
    use std::collections::{HashMap, LinkedList};
    use std::mem;
    use std::sync::Arc;

    #[derive(Message, Clone)]
    #[rtype(result = "()")]
    pub struct Initialize(pub Arc<crate::AppState>);

    pub struct ReceiverActor {
        id: u16,
        outbox_address: Addr<OutboxActor>,
        state: Option<Arc<crate::AppState>>,
        input_queues: Vec<Option<RoutedEpochBatch>>,
    }

    impl ReceiverActor {
        pub fn new(id: u16, outbox_address: Addr<OutboxActor>) -> Self {
            ReceiverActor {
                id,
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
                    let inbox_id = batch.inbox_id;
                    for mut message in batch.messages.into_iter() {
                        message.inbox_index =
                            (message.inbox_index & 0x0000FFFFFFFFFFFF) | ((inbox_id as u64) << 48);
                        let device_messages = output_map
                            .entry(message.payload.device_id.clone())
                            .or_insert_with(|| LinkedList::new());
                        device_messages.push_back((batch.epoch_id, message));
                    }
                }
                self.outbox_address.do_send(DeviceEpochBatch(epoch_id, output_map))
            }
        }
    }

    pub struct OutboxActor {
        id: u16,
        sequencer: Addr<crate::sequencer::SequencerActor>,
    }

    impl OutboxActor {
        pub fn new(id: u16, sequencer: Addr<crate::sequencer::SequencerActor>) -> Self {
            OutboxActor { id, sequencer }
        }
    }

    impl Actor for OutboxActor {
        type Context = Context<Self>;
    }

    #[derive(Message, Clone, Debug)]
    #[rtype(result = "()")]
    pub struct DeviceEpochBatch(pub u64, pub HashMap<String, LinkedList<(u64, RoutedMessage)>>);

    impl Handler<DeviceEpochBatch> for OutboxActor {
        type Result = ();

        fn handle(&mut self, msg: DeviceEpochBatch, _ctx: &mut Context<Self>) -> Self::Result {
            println!("Message: {:?}", msg);
            self.sequencer.do_send(crate::sequencer::EndEpoch(msg.0, self.id));
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
            for i in 0..1000000 {
                let x  = i*77 / 12;
            }

            self.epoch += 1;
            println!("Starting epoch {:?}", self.epoch);
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
                if !self.outbox_signals.contains(&outbox_id){
                    self.outbox_signals.push(outbox_id);
                }

                let num_outboxes = self.state.as_ref().unwrap().outbox_actors.len(); 
                if self.outbox_signals.len() == num_outboxes {
                    println!("ending epoch: {:?}", self.epoch);
                    self.epoch += 1;
                    println!("starting epoch: {:?}", self.epoch);
                    self.outbox_signals = Vec::new();
                    for inbox in self.state.as_ref().unwrap().inbox_actors.iter() {
                        inbox.do_send(crate::inbox::EpochStart(self.epoch));
                    }
                }
            }
        }
    }
}

const INBOX_ACTORS: u16 = 1;
const OUTBOX_ACTORS: u16 = 1;

pub struct AppState {
    inbox_actors: Vec<Addr<inbox::InboxActor>>,
    outbox_actors: Vec<(Addr<outbox::ReceiverActor>, Addr<outbox::OutboxActor>)>,
    sequencer: Addr<sequencer::SequencerActor>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    use actix::{Actor, Addr};

    let sequencer = sequencer::SequencerActor::new().start();

    // Boot up a set of inbox actors:
    let inbox_actors: Vec<Addr<inbox::InboxActor>> = (0..INBOX_ACTORS)
        .map(|id| inbox::InboxActor::new(id).start())
        .collect();

    // Boot up a set of outbox actors:
    let outbox_actors: Vec<(Addr<outbox::ReceiverActor>, Addr<outbox::OutboxActor>)> = (0
        ..OUTBOX_ACTORS)
        .map(|id| {
            let out_id = outbox::OutboxActor::new(id, sequencer.clone()).start();
            let rec_id = outbox::ReceiverActor::new(id, out_id.clone()).start();
            (rec_id, out_id)
        })
        .collect();
    

    let state = web::Data::new(AppState {
        sequencer: sequencer.clone(),
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
    
    sequencer.send(sequencer::Initialize(state.clone().into_inner())).await.unwrap();
    
    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .service(index)
            .service(hello)
            .service(handle_message)
            .service(incr_epoch)
    })
    .bind(("127.0.0.1", 8081))?
    .run()
    .await
}
