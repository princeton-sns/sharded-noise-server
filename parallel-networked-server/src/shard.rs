use actix::{Actor, Addr, Arbiter};
use actix_web::{
    delete, error, get, http::header, post, web, HttpMessage, HttpResponse, Responder,
};

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
    use serde::{Deserialize, Serialize};
    use std::borrow::Cow;
    use std::collections::LinkedList;
    use std::fs;
    use std::sync::Arc;

    #[derive(Serialize, Deserialize)]
    pub enum PersistRecord<'a> {
        Epoch(u64),
        Message(Cow<'a, super::client_protocol::OutboxMessage>),
    }

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
        pub (usize, LinkedList<Event>),
    );
    #[derive(Message, Clone, Debug)]
    #[rtype(result = "()")]
    pub struct RoutedEpochBatch {
        pub epoch_id: u64,
        pub inbox_id: u8,
        pub messages: Vec<super::client_protocol::OutboxMessage>,
    }

    pub struct RouterActor {
        id: u8,
        persist_file: fs::File,
    }

    impl RouterActor {
        pub fn new(id: u8) -> Self {
            RouterActor {
                id,
                persist_file: fs::OpenOptions::new()
                    .read(false)
                    .write(true)
                    .create_new(true)
                    .open(&format!("./persist-inbox/{}.bin", id))
                    .unwrap(),
            }
        }
    }

    impl Actor for RouterActor {
        type Context = Context<Self>;
    }

    impl Handler<InboxEpoch> for RouterActor {
        type Result = ();

        fn handle(&mut self, msg: InboxEpoch, _ctx: &mut Context<Self>) {
            use std::io::Write;

            let InboxEpoch(state, epoch_id, shard_id, (queue_length, queue)) = msg;

            // At least a few file system blocks, RAM is cheap
            let mut writer = std::io::BufWriter::with_capacity(128 * 4069, &mut self.persist_file);

            bincode::serialize_into(&mut writer, &PersistRecord::Epoch(epoch_id)).unwrap();

            // Allocate a bunch of vectors for the individual intershard
            // routers. We preallocate the maximum capacity (queue_length) for
            // each and rely on the OS performing lazy zero-page CoW allocation
            // to avoid excessive memory consumption. This allows us to avoid
            // per-message allocations (LinkedList) or re-allocating the vectors.
            let mut intershard = vec![vec![]; state.intershard_router_actors.len()];
            intershard.iter_mut().for_each(|v| v.reserve(queue_length));

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

                    bincode::serialize_into(
                        &mut writer,
                        &PersistRecord::Message(Cow::Borrowed(&rt_msg)),
                    )
                    .unwrap();

                    intershard[bucket].push(rt_msg);
                }
            }

            writer.flush().unwrap();

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
        queue: (usize, LinkedList<Event>),
        epoch: Option<u64>,
        router: Addr<RouterActor>,
        state: Option<Arc<super::ShardState>>,
    }

    impl InboxActor {
        pub fn new(id: u8, router: Addr<RouterActor>) -> Self {
            InboxActor {
                _id: id,
                queue: (0, LinkedList::new()),
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
                self.queue.1.push_back(ev);
                self.queue.0 += 1;
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
                // println!("cur_epoch: {}", cur_epoch);
                assert!(*cur_epoch + 1 == next_epoch);

                // Swap out the queue and send it to the router:
                let cur_queue = mem::replace(&mut self.queue, (0, LinkedList::new()));
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
    use actix::{Actor, Context, Handler, Message, ResponseFuture};
    use serde::{Deserialize, Serialize};

    use std::mem;

    use std::sync::Arc;

    #[derive(Message, Clone, Debug)]
    #[rtype(result = "()")]
    pub struct RoutedEpochBatch {
        pub src_shard_id: u8,
        pub dst_shard_id: u8,
        pub epoch_id: u64,
        pub messages: Vec<super::client_protocol::OutboxMessage>,
    }

    #[derive(Message, Serialize, Deserialize, Clone, Debug)]
    #[rtype(result = "()")]
    pub struct IntershardRoutedEpochBatch {
        pub src_shard_id: u8,
        pub dst_shard_id: u8,
        pub epoch_id: u64,
        pub messages: Vec<Vec<super::client_protocol::OutboxMessage>>,
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
                // Replace the input queus:
                let input_queues = mem::replace(
                    &mut self.input_queues,
                    (
                        0,
                        vec![None; self.state.as_ref().unwrap().inbox_actors.len()],
                    ),
                );

                // To avoid copying, build a 2D vector of input queues to serialize:
                let shard_batch = input_queues
                    .1
                    .into_iter()
                    .map(|oq| oq.unwrap().messages)
                    .collect();

                if self.src_shard_id == self.dst_shard_id {
                    distribute_intershard_batch(
                        self.state.as_ref().unwrap(),
                        IntershardRoutedEpochBatch {
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
                            .json(&IntershardRoutedEpochBatch {
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

    pub fn distribute_intershard_batch(
        state: &super::ShardState,
        batch: IntershardRoutedEpochBatch,
    ) {
        assert!(batch.dst_shard_id == state.shard_id);

        // Allocate a bunch of vectors for the individual outbox receivers. We
        // preallocate the maximum capacity (queue_length) for each and rely on
        // the OS performing lazy zero-page CoW allocation to avoid excessive
        // memory consumption. This allows us to avoid per-message allocations
        // (LinkedList) or re-allocating the vectors.
        let batch_size = batch.messages.iter().map(|v| v.len()).sum();
        let mut outboxes = vec![Vec::new(); state.outbox_actors.len()];
        outboxes.iter_mut().for_each(|v| v.reserve(batch_size));

        for message in batch.messages.into_iter().flat_map(|v| v.into_iter()) {
            let bucket = super::hash_into_bucket(&message.payload.device_id, outboxes.len(), false);
            outboxes[bucket].push(message);
        }

        // Now free up any unused memory. This shouldn't move the vector on the
        // heap, but perhaps enable some new allocations, especially if the
        // actual number of messages delivered to an outbox was very small.
        outboxes.iter_mut().for_each(|v| v.shrink_to_fit());

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

    pub struct EpochCollectorActor {
        state: Option<Arc<super::ShardState>>,
        epoch: Option<u64>,
        input_queues: (usize, Vec<Option<usize>>),
    }

    impl EpochCollectorActor {
        pub fn new() -> Self {
            EpochCollectorActor {
                state: None,
                epoch: None,
                input_queues: (0, vec![]),
            }
        }
    }

    impl Actor for EpochCollectorActor {
        type Context = Context<Self>;
    }

    impl Handler<Initialize> for EpochCollectorActor {
        type Result = ();

        fn handle(&mut self, msg: Initialize, _ctx: &mut Context<Self>) -> Self::Result {
            let Initialize(state) = msg;
            self.input_queues = (0, vec![None; state.outbox_actors.len()]);
            self.state = Some(state);
        }
    }

    impl Handler<super::outbox::EndEpoch> for EpochCollectorActor {
        type Result = ResponseFuture<()>;

        fn handle(
            &mut self,
            msg: super::outbox::EndEpoch,
            _ctx: &mut Context<Self>,
        ) -> Self::Result {
            let outbox_id = msg.1;
            let epoch = msg.0;
            let epoch_messages = msg.2;

            assert!(self.input_queues.1[outbox_id as usize].is_none());
            self.input_queues.1[outbox_id as usize] = Some(epoch_messages);
            self.input_queues.0 += 1;

            if let Some(cur_epoch) = self.epoch {
                assert!(cur_epoch == epoch);
            }
            self.epoch = Some(epoch);

            // check if every element of input_queus has been written to
            if self.input_queues.0 == self.input_queues.1.len() {
                let seq_url = self.state.as_ref().unwrap().sequencer_url.clone();
                let self_shard_id = self.state.as_ref().unwrap().shard_id;
                let httpc = self.state.as_ref().unwrap().httpc.clone();

                let received_messages = self.input_queues.1.iter().map(|orm| orm.unwrap()).sum();

                self.input_queues = (
                    0,
                    vec![None; self.state.as_ref().unwrap().outbox_actors.len()],
                );
                self.epoch = None;

                Box::pin(async move {
                    // println!("Finishing epoch {}", epoch);
                    httpc
                        .post(format!("{}/end-epoch", seq_url))
                        .json(&crate::sequencer::EndEpochReq {
                            shard_id: self_shard_id,
                            epoch_id: epoch,
                            received_messages,
                        })
                        .send()
                        .await
                        .unwrap();
                })
            } else {
                Box::pin(async move {})
            }
        }
    }
}

pub mod outbox {
    use actix::{Actor, Addr, Context, Handler, Message, MessageResponse};
    use actix_web_lab::sse::{self, ChannelStream, Sse};
    use std::collections::{HashMap, LinkedList};
    use std::mem;
    use std::sync::Arc;

    #[derive(Message, Clone)]
    #[rtype(result = "()")]
    pub struct EndEpoch(pub u64, pub u8, pub usize);

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

            // check if every element of input_queues has been written to
            if self.input_queues.0 == self.input_queues.1.len() {
                let mut output_map = HashMap::new();

                let input_queues = mem::replace(
                    &mut self.input_queues,
                    (0, vec![None; self.state.as_ref().unwrap().shard_map.len()]),
                );

                let epoch_message_count = input_queues
                    .1
                    .iter()
                    .map(|ov| ov.as_ref().map(|b| b.messages.len()).unwrap())
                    .sum();

                for b in input_queues.1.into_iter() {
                    let batch = b.unwrap();
                    for message in batch.messages.into_iter() {
                        // Use this instead of .entry().or_insert_with() to
                        // avoid unconditionally cloning the device_id string:
                        if !output_map.contains_key(&message.payload.device_id) {
                            output_map.insert(
                                message.payload.device_id.clone(),
                                Vec::with_capacity(epoch_message_count),
                            );
                        }

                        output_map
                            .get_mut(&message.payload.device_id)
                            .unwrap()
                            .push(message);
                    }
                }

                // Shrink back all inserted overallocated Vecs:
                output_map.iter_mut().for_each(|(_k, v)| v.shrink_to_fit());

                self.outbox_address.do_send(DeviceEpochBatch(
                    epoch_id,
                    output_map,
                    epoch_message_count,
                ))
            }
        }
    }

    pub struct OutboxActor {
        id: u8,
        next_epoch: u64,
        // Mapping from device key to the next epoch which has not
        // been exposed to the client, and all messages from including
        // this message)
        client_mailboxes: HashMap<
            String,
            (
                u64,
                LinkedList<(u64, Vec<super::client_protocol::OutboxMessage>)>,
            ),
        >,
        state: Option<Arc<super::ShardState>>,
        client_streams: HashMap<String, sse::Sender>,
    }

    impl OutboxActor {
        pub fn new(id: u8) -> Self {
            OutboxActor {
                id,
                next_epoch: 0,
                client_mailboxes: HashMap::new(),
                state: None,
                client_streams: HashMap::new(),
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
        pub HashMap<String, Vec<super::client_protocol::OutboxMessage>>,
        pub usize,
    );

    #[derive(MessageResponse)]
    pub struct DeviceMessages(pub LinkedList<super::client_protocol::OutboxMessage>);

    #[derive(Message, Clone, Debug)]
    #[rtype(result = "DeviceMessages")]
    pub struct GetDeviceMessages(pub String);

    #[derive(Message, Clone, Debug)]
    #[rtype(result = "DeviceMessageStream")]
    pub struct GetDeviceMessageStream(pub String);

    #[derive(MessageResponse, Debug)]
    pub struct DeviceMessageStream(pub Sse<ChannelStream>);

    #[derive(Message, Clone, Debug)]
    #[rtype(result = "DeviceMessages")]
    pub struct DeleteDeviceMessages(pub String, pub (u64, u64));

    #[derive(Message, Clone, Debug)]
    #[rtype(result = "DeviceMessages")]
    pub struct ClearDeviceMessages(pub String);

    impl Handler<Initialize> for OutboxActor {
        type Result = ();

        fn handle(&mut self, msg: Initialize, _ctx: &mut Context<Self>) -> Self::Result {
            self.state = Some(msg.0);
        }
    }

    impl Handler<DeviceEpochBatch> for OutboxActor {
        type Result = ();

        fn handle(
            &mut self,
            epoch_batch: DeviceEpochBatch,
            _ctx: &mut Context<Self>,
        ) -> Self::Result {
            let DeviceEpochBatch(epoch_id, device_messages, message_count) = epoch_batch;

            for (device, messages) in device_messages.into_iter() {
                if let Some(tx) = self.client_streams.get_mut(&device) {
                    tx.try_send(sse::Data::new("got message!")).unwrap();
                }

                if let Some((_, ref mut device_mailbox)) = self.client_mailboxes.get_mut(&device) {
                    device_mailbox.push_back((epoch_id, messages));
                } else {
                    let mut message_queue = LinkedList::new();
                    message_queue.push_back((epoch_id, messages));
                    self.client_mailboxes.insert(device, (0, message_queue));
                }
            }

            self.next_epoch = epoch_id + 1;

            self.state
                .as_ref()
                .unwrap()
                .epoch_collector_actor
                .do_send(super::outbox::EndEpoch(epoch_id, self.id, message_count));
        }
    }

    impl Handler<GetDeviceMessageStream> for OutboxActor {
        type Result = DeviceMessageStream;

        fn handle(
            &mut self,
            msg: GetDeviceMessageStream,
            _ctx: &mut Context<Self>,
        ) -> Self::Result {
            let GetDeviceMessageStream(client_id) = msg;

            // TODO: flush current messages onto the channel? This will probably
            // block for too long. We may want to provide the client some
            // indication the epoch at which we start streaming, and then the
            // client can fetch those messages through a separate endpoint.

            let (tx, rx) = sse::channel(10);
            self.client_streams.insert(client_id, tx);

            DeviceMessageStream(rx)
        }
    }

    impl Handler<GetDeviceMessages> for OutboxActor {
        type Result = DeviceMessages;

        fn handle(&mut self, msg: GetDeviceMessages, _ctx: &mut Context<Self>) -> Self::Result {
            let (ref mut _client_next_epoch, ref mut client_msgs) = self
                .client_mailboxes
                .entry(msg.0)
                .or_insert_with(|| (0, LinkedList::new()));

            DeviceMessages(
                client_msgs
                    .into_iter()
                    .flat_map(|(_, v)| v.into_iter().map(|m| m.clone()))
                    .collect(),
            )
        }
    }

    impl Handler<DeleteDeviceMessages> for OutboxActor {
        type Result = DeviceMessages;

        fn handle(&mut self, msg: DeleteDeviceMessages, _ctx: &mut Context<Self>) -> Self::Result {
            let (ref mut client_next_epoch, ref mut client_msgs) = self
                .client_mailboxes
                .entry(msg.0)
                .or_insert_with(|| (0, LinkedList::new()));

            *client_next_epoch = self.next_epoch;

            //FIX: use drain filter if we're ok with experimental
            //let leftover_msgs = client_msgs.drain_filter(|x| *x.seq < msg.1).collect::<LinkedList<_>>();

            let mut index = 0;
            for m in client_msgs.iter().flat_map(|(_, v)| v.iter()) {
                if m.seq < msg.1 {
                    index += 1;
                } else {
                    break;
                }
            }

            let leftover_msgs = client_msgs.split_off(index);
            let msgs = mem::replace(client_msgs, leftover_msgs);

            DeviceMessages(
                msgs.into_iter()
                    .flat_map(|(_, v)| v.into_iter().map(|m| m.clone()))
                    .collect(),
            )
        }
    }

    impl Handler<ClearDeviceMessages> for OutboxActor {
        type Result = DeviceMessages;

        fn handle(&mut self, msg: ClearDeviceMessages, _ctx: &mut Context<Self>) -> Self::Result {
            let (ref mut client_next_epoch, ref mut client_msgs) = self
                .client_mailboxes
                .entry(msg.0)
                .or_insert_with(|| (0, LinkedList::new()));

            *client_next_epoch = self.next_epoch;

            let msgs = mem::replace(client_msgs, LinkedList::new());

            DeviceMessages(
                msgs.into_iter()
                    .flat_map(|(_, v)| v.into_iter().map(|m| m.clone()))
                    .collect(),
            )
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

    let res = (hash % (bucket_count as u32)) as usize;

    res
}

#[get("/")]
async fn index() -> impl Responder {
    HttpResponse::NoContent().finish()
}

#[get("/shard")]
async fn outbox_shard(
    state: web::Data<ShardState>,
    auth: web::Header<BearerToken>,
) -> impl Responder {

    let device_id = auth.into_inner().into_token();
    let bucket = hash_into_bucket(&device_id, state.intershard_router_actors.len(), true);

    web::Json::<String>(state.shard_map[bucket].clone())
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

#[get("/events")]
async fn stream_messages(
    state: web::Data<ShardState>,
    auth: web::Header<BearerToken>,
) -> impl Responder {
    let device_id = auth.into_inner().into_token();
    let outbox_actors_cnt = state.outbox_actors.len();
    let actor_idx = hash_into_bucket(&device_id, outbox_actors_cnt, false);

    state.outbox_actors[actor_idx]
        .1
        .send(outbox::GetDeviceMessageStream(device_id))
        .await
        .unwrap()
        .0
}

#[delete("/outbox")]
async fn clear_messages(
    state: web::Data<ShardState>,
    auth: web::Header<BearerToken>,
) -> impl Responder {
    let device_id = auth.into_inner().into_token();
    let outbox_actors_cnt = state.outbox_actors.len();
    let actor_idx = hash_into_bucket(&device_id, outbox_actors_cnt, false);

    let messages = state.outbox_actors[actor_idx]
        .1
        .send(outbox::ClearDeviceMessages(device_id))
        .await
        .unwrap();

    web::Json(messages.0)
}

// TODO: find opt param setup and combine with above
#[delete("/outbox/{seq_high}/{seq_low}")]
async fn delete_messages(
    state: web::Data<ShardState>,
    auth: web::Header<BearerToken>,
    msg_id: web::Path<(u64, u64)>,
) -> impl Responder {
    let device_id = auth.into_inner().into_token();
    let outbox_actors_cnt = state.outbox_actors.len();
    let actor_idx = hash_into_bucket(&device_id, outbox_actors_cnt, false);

    let messages = state.outbox_actors[actor_idx]
        .1
        .send(outbox::DeleteDeviceMessages(device_id, *msg_id))
        .await
        .unwrap();

    web::Json(messages.0)
}

#[get("/outbox")]
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
    // println!("Received start_epoch request: {}", *epoch_id);
    for inbox in state.inbox_actors.iter() {
        inbox.do_send(inbox::EpochStart(*epoch_id));
    }
    ""
}

#[post("/intershard-batch")]
async fn intershard_batch(
    state: web::Data<ShardState>,
    batch: web::Json<intershard::IntershardRoutedEpochBatch>,
) -> impl Responder {
    intershard::distribute_intershard_batch(&*state, batch.into_inner());
    ""
}

pub struct ShardState {
    httpc: reqwest::Client,
    shard_id: u8,
    shard_map: Vec<String>,
    sequencer_url: String,
    inbox_actors: Vec<Addr<inbox::InboxActor>>,
    intershard_router_actors: Vec<Addr<intershard::InterShardRouterActor>>,
    outbox_actors: Vec<(Addr<outbox::ReceiverActor>, Addr<outbox::OutboxActor>)>,
    epoch_collector_actor: Addr<intershard::EpochCollectorActor>,
}

pub async fn init(
    shard_base_url: String,
    sequencer_base_url: String,
    inbox_count: u8,
    outbox_count: u8,
) -> impl Fn(&mut web::ServiceConfig) + Clone + Send + 'static {
    use std::fs;
    use std::io::{self, Write};
    use tokio::sync::mpsc;

    fs::create_dir("./persist-inbox").unwrap();

    let httpc = reqwest::Client::new();
    let mut arbiters = Vec::new();

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
            .get(format!("{}/shard-map", &sequencer_base_url))
            .send()
            .await;

        match resp {
            Ok(res) if res.status().is_success() => {
                shard_map_opt = Some(
                    res.json::<crate::sequencer::SequencerShardMap>()
                        .await
                        .unwrap(),
                );
            }
            _ => {
                print!(".");
                io::stdout().flush().unwrap();
                sleep(Duration::from_millis(500)).await;
            }
        }
    }
    println!("Retrieved shard map!");
    let shard_map = shard_map_opt.unwrap().shards;

    // Boot up a set of inbox actors on individual arbiters
    let mut inbox_actors: Vec<Addr<inbox::InboxActor>> = Vec::new();
    for id in 0..inbox_count {
        let (router_tx, mut router_rx) = mpsc::channel(1);
        let router_arbiter = Arbiter::new();
        assert!(router_arbiter.spawn(async move {
            let addr = inbox::RouterActor::new(id).start();
            router_tx.send(addr).await.unwrap();
        }));
        arbiters.push(router_arbiter);
        let router = router_rx.recv().await.unwrap();

        let (inbox_tx, mut inbox_rx) = mpsc::channel(1);
        let inbox_arbiter = Arbiter::new();
        assert!(inbox_arbiter.spawn(async move {
            let addr = inbox::InboxActor::new(id, router).start();
            inbox_tx.send(addr).await.unwrap();
        }));
        arbiters.push(inbox_arbiter);

        inbox_actors.push(inbox_rx.recv().await.unwrap());
    }

    // Boot up a set of intershard routers:
    let mut intershard_router_actors: Vec<Addr<intershard::InterShardRouterActor>> = Vec::new();
    for id in 0..shard_map.len() {
        let (isr_tx, mut isr_rx) = mpsc::channel(1);
        let isr_arbiter = Arbiter::new();
        assert!(isr_arbiter.spawn(async move {
            let addr =
                intershard::InterShardRouterActor::new(register_resp.shard_id, id as u8).start();
            isr_tx.send(addr).await.unwrap();
        }));
        arbiters.push(isr_arbiter);
        intershard_router_actors.push(isr_rx.recv().await.unwrap());
    }

    // Boot up a set of outbox actors on individual arbiters:
    let mut outbox_actors: Vec<(Addr<outbox::ReceiverActor>, Addr<outbox::OutboxActor>)> =
        Vec::new();
    for id in 0..outbox_count {
        let (outbox_tx, mut outbox_rx) = mpsc::channel(1);
        let outbox_arbiter = Arbiter::new();
        let _sequencer_clone = sequencer_base_url.clone();
        assert!(outbox_arbiter.spawn(async move {
            let addr = outbox::OutboxActor::new(id).start();
            outbox_tx.send(addr).await.unwrap();
        }));
        arbiters.push(outbox_arbiter);
        let outbox_addr = outbox_rx.recv().await.unwrap();

        let (recv_tx, mut recv_rx) = mpsc::channel(1);
        let recv_arbiter = Arbiter::new();
        let outbox_addr_clone = outbox_addr.clone();
        assert!(recv_arbiter.spawn(async move {
            let addr = outbox::ReceiverActor::new(id, outbox_addr_clone).start();
            recv_tx.send(addr).await.unwrap();
        }));
        arbiters.push(recv_arbiter);

        outbox_actors.push((recv_rx.recv().await.unwrap(), outbox_addr));
    }

    let (collector_tx, mut collector_rx) = mpsc::channel(1);
    let collector_arbiter = Arbiter::new();
    assert!(collector_arbiter.spawn(async move {
        let addr = intershard::EpochCollectorActor::new().start();
        collector_tx.send(addr).await.unwrap();
    }));
    arbiters.push(collector_arbiter);
    let epoch_collector_actor = collector_rx.recv().await.unwrap();

    let state = web::Data::new(ShardState {
        httpc,
        sequencer_url: sequencer_base_url,
        shard_id: register_resp.shard_id,
        shard_map,
        inbox_actors,
        intershard_router_actors,
        outbox_actors,
        epoch_collector_actor: epoch_collector_actor.clone(),
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
        outbox_actor
            .1
            .send(outbox::Initialize(state.clone().into_inner()))
            .await
            .unwrap();
    }

    epoch_collector_actor
        .send(intershard::Initialize(state.clone().into_inner()))
        .await
        .unwrap();

    Box::new(move |service_config: &mut web::ServiceConfig| {
        service_config
            .app_data(state.clone())
            // Client API
            .service(handle_message)
            .service(retrieve_messages)
            .service(delete_messages)
            .service(clear_messages)
            .service(stream_messages)
	    .service(outbox_shard)
            // Sequencer API
            .service(start_epoch)
            .service(index)
            // Intershard API
            .service(intershard_batch);
    })
    }
