use actix::{Actor, Addr, Arbiter};
use actix_web::{
    delete, error, get, http::header, post, web, HttpMessage, HttpRequest, HttpResponse, Responder,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use tokio::time::{sleep, Duration};

const ACTOR_MAILBOX_CAP: usize = 1024;

pub mod client_protocol {
    use serde::{Deserialize, Serialize};
    use std::borrow::{Borrow, Cow};
    use std::collections::BTreeMap;

    // -------------------------------------------------------------------------
    // HTTP API Types

    #[derive(Debug, Serialize, Deserialize, Clone)]
    #[serde(transparent)]
    pub struct EncryptedCommonPayload(pub String);

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub struct EncryptedPerRecipientPayload {
        pub c_type: usize,
        pub ciphertext: String,
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub struct EncryptedOutboxMessage {
        pub enc_common: EncryptedCommonPayload,
        pub enc_recipients: BTreeMap<String, EncryptedPerRecipientPayload>,
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub struct EncryptedInboxMessage {
        pub sender: String,
        // Must be sorted in key order
        pub recipients: Vec<String>,
        pub enc_common: EncryptedCommonPayload,
        pub enc_recipient: EncryptedPerRecipientPayload,
        pub seq_id: u128,
    }

    // -------------------------------------------------------------------------
    // EventSource Messages

    #[derive(Serialize, Clone, Debug)]
    pub struct EpochMessageBatch<'a> {
        pub epoch_id: u64,
        pub messages: Cow<'a, Vec<EncryptedInboxMessage>>,
        pub attestation: String,
    }

    #[derive(Serialize, Clone, Debug)]
    #[serde(rename_all = "camelCase")]
    pub struct OtkeyRequest {
        pub device_id: String,
        pub needs: usize,
    }

    // -------------------------------------------------------------------------
    // Attestation payload layout

    #[derive(Clone, Debug)]
    // Format:
    // - u64: first_epoch
    // - u64: next_epoch
    // - [u8; 32]: messages_digest
    pub struct AttestationData([u8; 48]);

    impl AttestationData {
        fn message_hash_table<T: Borrow<EncryptedInboxMessage>>(
            messages: impl Iterator<Item = T>,
        ) -> impl Iterator<Item = (u128, [u8; 32], [u8; 32])> {
            use sha2::{Digest, Sha256};

            let mut hasher = Sha256::new();

            messages.map(move |message| {
                for r in message.borrow().recipients.iter() {
                    hasher.update(r.as_bytes());
                    hasher.update(b";");
                }
                let mut recipients_digest = [0; 32];
                hasher.finalize_into_reset((&mut recipients_digest).into());

                let mut payload_digest = [0; 32];
                hasher.update(message.borrow().enc_common.0.as_bytes());
                hasher.finalize_into_reset((&mut payload_digest).into());

                (message.borrow().seq_id, recipients_digest, payload_digest)
            })
        }

        pub fn from_inbox_epochs<T: Borrow<EncryptedInboxMessage>>(
            client_id: &str,
            first_epoch: u64,
            next_epoch: u64,
            messages: impl Iterator<Item = T>,
        ) -> AttestationData {
            Self::from_inbox_epochs_int(
                client_id,
                first_epoch,
                next_epoch,
                Self::message_hash_table(messages),
            )
        }

        fn from_inbox_epochs_int<T: Borrow<(u128, [u8; 32], [u8; 32])>>(
            client_id: &str,
            first_epoch: u64,
            next_epoch: u64,
            message_hash_table: impl Iterator<Item = T>,
        ) -> AttestationData {
            use sha2::{Digest, Sha256};

            let mut hasher = Sha256::new();

            hasher.update(client_id.as_bytes());

            message_hash_table.for_each(|b| {
                let (epoch_id, recipients_digest, payload_digest) = b.borrow();
                hasher.update(&u128::to_le_bytes(*epoch_id));
                hasher.update(&recipients_digest);
                hasher.update(&payload_digest);
            });

            let mut attestation_messages_hash = [0; 32];
            hasher.finalize_into((&mut attestation_messages_hash).into());

            let mut attestation_data = [0; 48];
            attestation_data[0..8].copy_from_slice(&u64::to_le_bytes(first_epoch));
            attestation_data[8..16].copy_from_slice(&u64::to_le_bytes(next_epoch));
            attestation_data[16..].copy_from_slice(&attestation_messages_hash);
            AttestationData(attestation_data)
        }

        pub fn attest(&self, attestation_key: &ed25519_dalek::Keypair) -> Attestation {
            use ed25519_dalek::Signer;
            let signature = attestation_key.sign(&self.0);

            let mut attestation = [0; 48 + 64];
            attestation[0..48].copy_from_slice(&self.0);
            attestation[48..].copy_from_slice(&signature.to_bytes());
            Attestation(attestation)
        }

        pub fn produce_claim(
            &self,
            client_id: String,
            first_epoch: u64,
            next_epoch: u64,
            messages: Vec<EncryptedInboxMessage>,
            claim_message_idx: Option<usize>,
            attestation: Attestation,
        ) -> AttestationClaim {
            if let Some(idx) = claim_message_idx {
                assert!(idx < messages.len());
            }

            // Generate the hash-table to be passed alongside the attestation:
            let message_hash_table: Vec<_> = Self::message_hash_table(messages.iter()).collect();

            // Sanity check that the claim we're producing is supported by the
            // passed attestation:
            let attestation_data = Self::from_inbox_epochs_int(
                &client_id,
                first_epoch,
                next_epoch,
                message_hash_table.iter(),
            );
            assert!(attestation_data.0[..] == attestation.0[..48]);

            // Extract the server signature from the attestation:
            let mut signature = [0; 64];
            signature.copy_from_slice(&attestation.0[48..]);

            AttestationClaim {
                client_id,
                first_epoch,
                next_epoch,
                message_hash_table,
                message_recipients: claim_message_idx.map(|idx| {
                    // Need to clone, can't move out of Vec
                    (idx, messages[idx].recipients.clone())
                }),
                attestation: signature,
            }
        }
    }

    #[derive(Clone, Debug)]
    pub struct AttestationClaim {
        pub client_id: String,
        pub first_epoch: u64,
        pub next_epoch: u64,
        pub message_hash_table: Vec<(u128, [u8; 32], [u8; 32])>,
        pub message_recipients: Option<(usize, Vec<String>)>,
        pub attestation: [u8; 64],
    }

    impl AttestationClaim {
        pub fn attestation(&self) -> Attestation {
            let attestation_data = AttestationData::from_inbox_epochs_int(
                &self.client_id,
                self.first_epoch,
                self.next_epoch,
                self.message_hash_table.iter(),
            );
            let mut attestation_buf = [0; 48 + 64];
            attestation_buf[..48].copy_from_slice(&attestation_data.0);
            attestation_buf[48..].copy_from_slice(&self.attestation);
            Attestation(attestation_buf)
        }

        pub fn supports(
            &self,
            other: &AttestationClaim,
            public_key: &ed25519_dalek::PublicKey,
        ) -> bool {
            // Two claims support each other when
            // - one contains a message at a sequence number that the other
            //   does not (positive + negative claim), or
            // - both contain a message with an identical sequence number but
            //   differing receipients or common payload (positive + positive).
            //
            // For simpler handling, "sort" the two claims by whichever contains
            // a potentially conflicting message reference. It's invalid for
            // neither of them to point to a message:
            let (claim_p, claim_pn) = if self.message_recipients.is_some() {
                (self, other)
            } else {
                (other, self)
            };

            let (p_idx, claim_p_recipients) = if let Some(p) = &claim_p.message_recipients {
                p
            } else {
                // At least one positive claim required!
                return false;
            };

            // Need to handle positive + positive & positive + negative
            // differently:
            if let Some((pn_idx, _recipients)) = &claim_pn.message_recipients {
                // Positive + positive claim! Compare sequence numbers. If they
                // match, recipients and payload must be identical.
                let (p_seqid, p_recipients, p_payload) = claim_p.message_hash_table[*p_idx];
                let (pn_seqid, pn_recipients, pn_payload) = claim_pn.message_hash_table[*pn_idx];

                if p_seqid != pn_seqid || (p_recipients == pn_recipients && p_payload == pn_payload)
                {
                    return false;
                }

            // Claims support each other, but individual claims not verified yet!
            } else {
                // Positive + negative claim! The positive claim can only be
                // supported by the negative claim if the offending message's
                // sequence number is contained within the negative claim's
                // sequence space, so verify that.
                let (p_seqid, p_recipients, _p_payload) = claim_p.message_hash_table[*p_idx];

                // Is there a better way to do this which doesn't involve
                // bitshifting & casting?
                let [e0, e1, e2, e3, e4, e5, e6, e7, _, _, _, _, _, _, _, _] =
                    u128::to_be_bytes(p_seqid);
                let p_epoch = u64::from_be_bytes([e0, e1, e2, e3, e4, e5, e6, e7]);
                if p_epoch < claim_pn.first_epoch || p_epoch >= claim_pn.next_epoch {
                    return false;
                }

                // claim_p's message lies in claim_pn's sequence space, now we
                // need to verify that claim_p should indeed have been received
                // by claim_pn. We can do this by ensuring that claim_pn's
                // client_id is in the recipients of claim_p's message.
                //
                // For this, we first have to validate that the
                // message_hash_table entry's recipients hash corresponds to the
                // recipients vec part of the claim, and then check that
                // claim_pn's client id is an element in that vec:
                let claim_p_recipients_digest = {
                    use sha2::{Digest, Sha256};
                    let mut hasher = Sha256::new();
                    for r in claim_p_recipients {
                        hasher.update(r.as_bytes());
                        hasher.update(b";");
                    }
                    let mut recipients_digest = [0; 32];
                    hasher.finalize_into_reset((&mut recipients_digest).into());
                    recipients_digest
                };

                if p_recipients != claim_p_recipients_digest {
                    // Can't reproduce the hash-table entry of the recipients of
                    // claim_p's message.
                    return false;
                }

                // p_recipients indeed corresponds to claim_p's
                // claim_p_recipients entry:
                if claim_p_recipients
                    .iter()
                    .find(|r| **r == claim_pn.client_id)
                    .is_none()
                {
                    // claim_pn's client_id isn't in the recipients list, so the
                    // negative claim can't be used here.
                    return false;
                }

                // Now, loop over claim_pn's message to ensure that it indeed
                // does not contain a message with the offending sequence
                // number:
                if claim_pn
                    .message_hash_table
                    .iter()
                    .find(|(seqid, _, _)| *seqid == p_seqid)
                    .is_some()
                {
                    return false;
                }

                // Claims support each other, but individual claims not verified yet!
            }

            // Verify each of the invidual claim's cryptographic validity. If
            // they are both valid, we hold evidence that the server has behaved
            // incorrectly.
            //
            // It is fine to use verify_trusted here, as we're trusting the
            // attestation payload hash which is constructed based on the claim
            // data by `attestation()`.
            claim_p.attestation().verify_trusted(public_key)
                && claim_pn.attestation().verify_trusted(public_key)
        }
    }

    #[derive(Clone, Debug)]
    pub struct Attestation([u8; 48 + 64]);

    impl Attestation {
        pub fn into_arr(self) -> [u8; 48 + 64] {
            self.0
        }

        pub fn from_arr(arr: [u8; 48 + 64]) -> Self {
            Attestation(arr)
        }

        pub fn from_bytes(bytes: &[u8]) -> Result<Self, ()> {
            if bytes.len() != 48 + 64 {
                Err(())
            } else {
                let mut buf = [0; 48 + 64];
                buf.copy_from_slice(bytes);
                Ok(Attestation(buf))
            }
        }

        pub fn decode_base64(s: &str) -> Result<Self, ()> {
            use base64::{engine::general_purpose, Engine as _};

            if let Ok(vec) = general_purpose::STANDARD_NO_PAD.decode(s) {
                if vec.len() != 48 + 64 {
                    Err(())
                } else {
                    let mut buf = [0; 48 + 64];
                    buf.copy_from_slice(&vec);
                    Ok(Attestation(buf))
                }
            } else {
                Err(())
            }
        }

        pub fn encode_base64(&self) -> String {
            use base64::{engine::general_purpose, Engine as _};
            general_purpose::STANDARD_NO_PAD.encode(&self.0)
        }

        pub fn first_epoch(&self) -> u64 {
            u64::from_le_bytes([
                self.0[0], self.0[1], self.0[2], self.0[3], self.0[4], self.0[5], self.0[6],
                self.0[7],
            ])
        }

        pub fn next_epoch(&self) -> u64 {
            u64::from_le_bytes([
                self.0[8], self.0[9], self.0[10], self.0[11], self.0[12], self.0[13], self.0[14],
                self.0[15],
            ])
        }

        pub fn verify(
            &self,
            data: &AttestationData,
            public_key: &ed25519_dalek::PublicKey,
        ) -> bool {
            data.0 == self.0[0..48] && self.verify_trusted(public_key)
        }

        pub fn verify_trusted(&self, public_key: &ed25519_dalek::PublicKey) -> bool {
            use ed25519_dalek::Verifier;
            let signature = ed25519_dalek::Signature::from_bytes(&self.0[48..]).unwrap();
            public_key.verify(&self.0[..48], &signature).is_ok()
        }
    }
}

pub mod outbox {
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
        Message(Cow<'a, Event>),
    }

    #[derive(Message, Clone)]
    #[rtype(result = "()")]
    pub struct Initialize(pub Arc<super::ShardState>);

    #[derive(Message, Debug, Clone)]
    #[rtype(result = "()")]
    pub struct EpochStart(pub u64);

    #[derive(Message, Serialize, Deserialize, Debug, Clone)]
    #[rtype(result = "Result<u64, EventError>")]
    pub struct Event {
        pub sender: String,
        pub message: super::client_protocol::EncryptedOutboxMessage,
    }

    #[derive(Debug)]
    pub enum EventError {
        NoEpoch,
    }

    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct OutboxEpoch(
        pub Arc<super::ShardState>,
        pub u64,
        pub u8,
        pub (usize, LinkedList<Event>),
    );
    #[derive(Message, Clone, Debug)]
    #[rtype(result = "()")]
    pub struct RoutedEpochBatch {
        pub epoch_id: u64,
        pub outbox_id: u8,
        pub messages: Vec<(String, super::client_protocol::EncryptedInboxMessage)>,
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
                    .open(&format!("./persist-outbox/{}.bin", id))
                    .unwrap(),
            }
        }
    }

    impl Actor for RouterActor {
        type Context = Context<Self>;

        fn started(&mut self, ctx: &mut Self::Context) {
            ctx.set_mailbox_capacity(super::ACTOR_MAILBOX_CAP);
        }
    }

    impl Handler<OutboxEpoch> for RouterActor {
        type Result = ();

        fn handle(&mut self, msg: OutboxEpoch, _ctx: &mut Context<Self>) {
            use std::io::Write;

            let OutboxEpoch(state, epoch_id, shard_id, (queue_length, queue)) = msg;

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
                // By means of this being a BTreeMap, the recipients
                // will be sorted in key order
                let receivers: Vec<_> = ev.message.enc_recipients.keys().cloned().collect();

                let seq = (epoch_id as u128) << 64
                    | ((idx as u128) & 0x0000FFFFFFFFFFFF)
                    | ((self.id as u128) << 48)
                    | ((shard_id as u128) << 56);

                bincode::serialize_into(&mut writer, &PersistRecord::Message(Cow::Borrowed(&ev)))
                    .unwrap();

                for (recipient, enc_recipient_payload) in ev.message.enc_recipients.into_iter() {
                    let bucket = hash_into_bucket(&recipient, intershard.len(), true);

                    let ibmsg = super::client_protocol::EncryptedInboxMessage {
                        sender: ev.sender.clone(),
                        recipients: receivers.clone(),
                        enc_common: ev.message.enc_common.clone(),
                        enc_recipient: enc_recipient_payload,
                        seq_id: seq,
                    };

                    intershard[bucket].push((recipient, ibmsg));
                }
            }

            writer.flush().unwrap();

            for (mut messages, intershard_rt) in intershard
                .into_iter()
                .zip(state.intershard_router_actors.iter())
            {
		messages.shrink_to_fit();

                let routed_batch = RoutedEpochBatch {
                    epoch_id,
                    outbox_id: self.id,
                    messages,
                };

                intershard_rt.do_send(routed_batch);
            }
        }
    }

    pub struct OutboxActor {
        _id: u8,
        queue: (usize, LinkedList<Event>),
        epoch: Option<u64>,
        router: Addr<RouterActor>,
        state: Option<Arc<super::ShardState>>,
    }

    impl OutboxActor {
        pub fn new(id: u8, router: Addr<RouterActor>) -> Self {
            OutboxActor {
                _id: id,
                queue: (0, LinkedList::new()),
                epoch: None,
                router,
                state: None,
            }
        }
    }

    impl Actor for OutboxActor {
        type Context = Context<Self>;

        fn started(&mut self, ctx: &mut Self::Context) {
            ctx.set_mailbox_capacity(super::ACTOR_MAILBOX_CAP);
        }
    }

    impl Handler<Initialize> for OutboxActor {
        type Result = ();

        fn handle(&mut self, msg: Initialize, _ctx: &mut Context<Self>) -> Self::Result {
            self.state = Some(msg.0);
        }
    }

    impl Handler<Event> for OutboxActor {
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

    impl Handler<EpochStart> for OutboxActor {
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
                self.router.do_send(OutboxEpoch(
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
        pub messages: Vec<(String, super::client_protocol::EncryptedInboxMessage)>,
    }

    #[derive(Message, Serialize, Deserialize, Clone, Debug)]
    #[rtype(result = "()")]
    pub struct IntershardRoutedEpochBatch {
        pub src_shard_id: u8,
        pub dst_shard_id: u8,
        pub epoch_id: u64,
        pub messages: Vec<Vec<(String, super::client_protocol::EncryptedInboxMessage)>>,
    }

    #[derive(Message, Clone)]
    #[rtype(result = "()")]
    pub struct Initialize(pub Arc<super::ShardState>);

    pub struct InterShardRouterActor {
        src_shard_id: u8,
        dst_shard_id: u8,
        state: Option<Arc<super::ShardState>>,
        input_queues: (usize, Vec<Option<super::outbox::RoutedEpochBatch>>),
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

        fn started(&mut self, ctx: &mut Self::Context) {
            ctx.set_mailbox_capacity(super::ACTOR_MAILBOX_CAP);
        }
    }

    impl Handler<Initialize> for InterShardRouterActor {
        type Result = ();

        fn handle(&mut self, msg: Initialize, _ctx: &mut Context<Self>) -> Self::Result {
            let Initialize(state) = msg;
            self.input_queues = (0, vec![None; state.outbox_actors.len()]);
            self.state = Some(state);
        }
    }

    impl Handler<super::outbox::RoutedEpochBatch> for InterShardRouterActor {
        type Result = ResponseFuture<()>;

        fn handle(
            &mut self,
            msg: super::outbox::RoutedEpochBatch,
            _ctx: &mut Context<Self>,
        ) -> Self::Result {
            let outbox_id = msg.outbox_id as usize;
            let epoch_id = msg.epoch_id as u64;
            assert!(self.input_queues.1[outbox_id].is_none());
            self.input_queues.1[outbox_id] = Some(msg);
            self.input_queues.0 += 1;

            // check if every element of input_queus has been written to
            if self.input_queues.0 == self.input_queues.1.len() {
                // Replace the input queus:
                let input_queues = mem::replace(
                    &mut self.input_queues,
                    (
                        0,
                        vec![None; self.state.as_ref().unwrap().outbox_actors.len()],
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

                    let payload = bincode::serialize(&IntershardRoutedEpochBatch {
                        src_shard_id: src_shard_id,
                        dst_shard_id: dst_shard_id,
                        epoch_id: epoch_id,
                        messages: shard_batch,
                    })
                    .unwrap();
                    Box::pin(async move {
                        let resp = httpc
                            .post(format!("{}/intershard-batch", dst_shard_url))
                            .body(payload)
                            .send()
                            .await
                            .unwrap();

                        if !resp.status().is_success() {
                            panic!("Received non-success on /intershard-batch: {:?}", resp);
                        }
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

        // Allocate a bunch of vectors for the individual inbox receivers. We
        // preallocate the maximum capacity (queue_length) for each and rely on
        // the OS performing lazy zero-page CoW allocation to avoid excessive
        // memory consumption. This allows us to avoid per-message allocations
        // (LinkedList) or re-allocating the vectors.
        let batch_size = batch.messages.iter().map(|v| v.len()).sum();
        let mut inboxes = vec![Vec::new(); state.inbox_actors.len()];
        inboxes.iter_mut().for_each(|v| v.reserve(batch_size));

        for message in batch.messages.into_iter().flat_map(|v| v.into_iter()) {
            let bucket = super::hash_into_bucket(&message.0, inboxes.len(), false);
            inboxes[bucket].push(message);
        }

        // Now free up any unused memory. This shouldn't move the vector on the
        // heap, but perhaps enable some new allocations, especially if the
        // actual number of messages delivered to an inbox was very small.
        inboxes.iter_mut().for_each(|v| v.shrink_to_fit());

        for (messages, inbox) in inboxes.into_iter().zip(state.inbox_actors.iter()) {
            let routed_batch = RoutedEpochBatch {
                epoch_id: batch.epoch_id,
                src_shard_id: batch.src_shard_id,
                dst_shard_id: state.shard_id,
                messages,
            };

            inbox.0.do_send(routed_batch);
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

        fn started(&mut self, ctx: &mut Self::Context) {
            ctx.set_mailbox_capacity(super::ACTOR_MAILBOX_CAP);
        }
    }

    impl Handler<Initialize> for EpochCollectorActor {
        type Result = ();

        fn handle(&mut self, msg: Initialize, _ctx: &mut Context<Self>) -> Self::Result {
            let Initialize(state) = msg;
            self.input_queues = (0, vec![None; state.inbox_actors.len()]);
            self.state = Some(state);
        }
    }

    impl Handler<super::inbox::EndEpoch> for EpochCollectorActor {
        type Result = ResponseFuture<()>;

        fn handle(
            &mut self,
            msg: super::inbox::EndEpoch,
            _ctx: &mut Context<Self>,
        ) -> Self::Result {
            let inbox_id = msg.1;
            let epoch = msg.0;
            let epoch_messages = msg.2;

            assert!(self.input_queues.1[inbox_id as usize].is_none());
            self.input_queues.1[inbox_id as usize] = Some(epoch_messages);
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
                    vec![None; self.state.as_ref().unwrap().inbox_actors.len()],
                );
                self.epoch = None;

                Box::pin(async move {
                    // println!("Finishing epoch {}", epoch);
                    let resp = httpc
                        .post(format!("{}/end-epoch", seq_url))
                        .json(&crate::sequencer::EndEpochReq {
                            shard_id: self_shard_id,
                            epoch_id: epoch,
                            received_messages,
                        })
                        .send()
                        .await
                        .unwrap();
                    if !resp.status().is_success() {
                        panic!("Received non-success on /end-epoch: {:?}", resp);
                    }
                })
            } else {
                Box::pin(async move {})
            }
        }
    }
}

pub mod inbox {
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
        inbox_address: Addr<InboxActor>,
        state: Option<Arc<super::ShardState>>,
        input_queues: (usize, Vec<Option<super::intershard::RoutedEpochBatch>>),
    }

    impl ReceiverActor {
        pub fn new(id: u8, inbox_address: Addr<InboxActor>) -> Self {
            ReceiverActor {
                _id: id,
                inbox_address,
                state: None,
                input_queues: (0, Vec::new()),
            }
        }
    }

    impl Actor for ReceiverActor {
        type Context = Context<Self>;

        fn started(&mut self, ctx: &mut Self::Context) {
            ctx.set_mailbox_capacity(super::ACTOR_MAILBOX_CAP);
        }
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
                        output_map
                            .entry(message.0)
                            .or_insert_with(|| Vec::with_capacity(epoch_message_count))
                            .push(message.1);
                        // // Use this instead of .entry().or_insert_with() to
                        // // avoid unconditionally cloning the device_id string:
                        // if !output_map.contains_key(&message.0) {
                        //     output_map.insert(
                        //         message.0.clone(),
                        //         Vec::with_capacity(epoch_message_count),
                        //     );
                        // }

                        // output_map
                        //     .get_mut(&message.0)
                        //     .unwrap()
                        //     .push(message);
                    }
                }

                // Shrink back all inserted overallocated Vecs:
                output_map.iter_mut().for_each(|(_k, v)| v.shrink_to_fit());

                self.inbox_address.do_send(DeviceEpochBatch(
                    epoch_id,
                    output_map,
                    epoch_message_count,
                ))
            }
        }
    }

    pub struct InboxActor {
        id: u8,
        next_epoch: u64,
        // Mapping from device key to the next epoch which has not
        // been exposed to the client, and all messages from including
        // this message)
        client_mailboxes: HashMap<
            String,
            (
                u64,
                LinkedList<(u64, Vec<super::client_protocol::EncryptedInboxMessage>)>,
            ),
        >,
        state: Option<Arc<super::ShardState>>,
        client_streams: HashMap<String, sse::Sender>,
        otkeys: HashMap<String, HashMap<String, String>>,
    }

    impl InboxActor {
        pub fn new(id: u8) -> Self {
            InboxActor {
                id,
                next_epoch: 0,
                client_mailboxes: HashMap::new(),
                state: None,
                client_streams: HashMap::new(),
                otkeys: HashMap::new(),
            }
        }

        pub fn request_otkeys(&mut self, client_id: String) {
            println!("Requesting 20 new otkeys for \"{}\"", client_id);
            if let Some(tx) = self.client_streams.get_mut(&client_id) {
                tx.try_send(
                    sse::Data::new_json(super::client_protocol::OtkeyRequest {
                        device_id: client_id,
                        needs: 20,
                    })
                    .unwrap()
                    .event("otkey"),
                )
                .unwrap();
            }
        }
    }

    impl Actor for InboxActor {
        type Context = Context<Self>;

        fn started(&mut self, ctx: &mut Self::Context) {
            ctx.set_mailbox_capacity(super::ACTOR_MAILBOX_CAP);
        }
    }

    #[derive(Message, Clone, Debug)]
    #[rtype(result = "()")]
    pub struct DeviceEpochBatch(
        pub u64,
        pub HashMap<String, Vec<super::client_protocol::EncryptedInboxMessage>>,
        pub usize,
    );

    #[derive(MessageResponse)]
    pub struct DeviceMessages(pub LinkedList<super::client_protocol::EncryptedInboxMessage>);

    #[derive(Message, Clone, Debug)]
    #[rtype(result = "DeviceMessages")]
    pub struct GetDeviceMessages(pub String);

    #[derive(Message, Clone, Debug)]
    #[rtype(result = "()")]
    pub struct ClearAllMessages;

    #[derive(Message, Clone, Debug)]
    #[rtype(result = "DeviceMessageStream")]
    pub struct GetDeviceMessageStream(pub String);

    #[derive(MessageResponse, Debug)]
    pub struct DeviceMessageStream(pub Sse<ChannelStream>);

    #[derive(Message, Clone, Debug)]
    #[rtype(result = "()")]
    pub struct AddOtkeys(pub String, pub HashMap<String, String>);

    #[derive(Message, Clone, Debug)]
    #[rtype(result = "Otkey")]
    pub struct GetOtkey(pub String);

    #[derive(MessageResponse, Clone, Debug)]
    pub struct Otkey(pub Option<(String, String)>);

    impl Handler<AddOtkeys> for InboxActor {
        type Result = ();

        fn handle(&mut self, msg: AddOtkeys, _ctx: &mut Context<Self>) -> Self::Result {
            let AddOtkeys(client_id, key_map) = msg;

            self.otkeys
                .entry(client_id)
                .or_insert_with(|| HashMap::new())
                .extend(key_map);
        }
    }

    impl Handler<GetOtkey> for InboxActor {
        type Result = Otkey;

        fn handle(&mut self, msg: GetOtkey, _ctx: &mut Context<Self>) -> Self::Result {
            let GetOtkey(client_id) = msg;

            let (res, key_map_len) = if let Some(key_map) = self.otkeys.get_mut(&client_id) {
                if let Some(k) = key_map.keys().next().cloned() {
                    let v = key_map.remove(&k).unwrap();
                    (Otkey(Some((k, v))), key_map.len())
                } else {
                    (Otkey(None), 0)
                }
            } else {
                (Otkey(None), 0)
            };

            if key_map_len < 10 {
                self.request_otkeys(client_id);
            }

            res
        }
    }

    impl Handler<ClearAllMessages> for InboxActor {
	type Result = ();
	fn handle(&mut self, _msg: ClearAllMessages, _ctx: &mut Context<Self>) -> Self::Result {
	    self.client_mailboxes = HashMap::new();
	}
    }

    // #[derive(Message, Clone, Debug)]
    // #[rtype(result = "DeviceMessages")]
    // pub struct DeleteDeviceMessages(pub String, pub (u64, u64));

    #[derive(Message, Clone, Debug)]
    #[rtype(result = "DeviceMessages")]
    pub struct ClearDeviceMessages(pub String);

    impl Handler<Initialize> for InboxActor {
        type Result = ();

        fn handle(&mut self, msg: Initialize, _ctx: &mut Context<Self>) -> Self::Result {
            self.state = Some(msg.0);
        }
    }

    impl Handler<DeviceEpochBatch> for InboxActor {
        type Result = ();

        fn handle(
            &mut self,
            epoch_batch: DeviceEpochBatch,
            _ctx: &mut Context<Self>,
        ) -> Self::Result {
            let DeviceEpochBatch(epoch_id, device_messages, message_count) = epoch_batch;

            for (device, messages) in device_messages.into_iter() {
                // Insert the messages into the data structure and immediately get a reference to it:
                let (prev_epoch, current_epoch_messages_ref) =
                    if let Some((_, ref mut device_mailbox)) =
                        self.client_mailboxes.get_mut(&device)
                    {
                        device_mailbox.push_back((epoch_id, messages));
                        (
                            device_mailbox.iter().nth_back(1).map(|(epoch, _)| epoch),
                            &device_mailbox.back().unwrap().1,
                        )
                    } else {
                        let mut message_queue = LinkedList::new();
                        message_queue.push_back((epoch_id, messages));
                        self.client_mailboxes
                            .insert(device.clone(), (0, message_queue));
                        (
                            None,
                            &self
                                .client_mailboxes
                                .get(&device)
                                .unwrap()
                                .1
                                .back()
                                .unwrap()
                                .1,
                        )
                    };

                if let Some(tx) = self.client_streams.get_mut(&device) {
                    use std::borrow::Cow;

                    let epoch_batch = super::client_protocol::EpochMessageBatch {
                        epoch_id,
                        messages: Cow::Borrowed(current_epoch_messages_ref),
                        attestation: super::client_protocol::AttestationData::from_inbox_epochs(
                            &device,
                            prev_epoch.map(|eid| eid + 1).unwrap_or(0),
                            epoch_id + 1,
                            current_epoch_messages_ref.iter(),
                            // Primitive test for missing messages in attestation:
                            // [].iter(),
                            // Primitive test for spurious message in attestation:
                            // current_epoch_messages_ref.iter().chain(
                            //     [super::client_protocol::EncryptedInboxMessage {
                            //         sender: "foo".to_string(),
                            //         recipients: vec!["bar".to_string()],
                            //         enc_common: super::client_protocol::EncryptedCommonPayload(
                            //             "hello!".to_string(),
                            //         ),
                            //         enc_recipient:
                            //             super::client_protocol::EncryptedPerRecipientPayload {
                            //                 c_type: 0,
                            //                 ciphertext: "very secret...".to_string(),
                            //             },
                            //         seq_id: 12345,
                            //     }]
                            //     .iter(),
                            // ),
                        )
                        .attest(&self.state.as_ref().unwrap().attestation_key)
                        .encode_base64(),
                    };

                    tx.try_send(
                        sse::Data::new_json(epoch_batch)
                            .unwrap()
                            .event("epoch_message_batch"),
                    )
                    .unwrap();

                    println!(
                        "Sent epoch batch SSE for client {} and epoch {}",
                        &device, epoch_id
                    );
                }
            }

            self.next_epoch = epoch_id + 1;

            self.state
                .as_ref()
                .unwrap()
                .epoch_collector_actor
                .do_send(super::inbox::EndEpoch(epoch_id, self.id, message_count));
        }
    }

    impl Handler<GetDeviceMessageStream> for InboxActor {
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

            let (tx, rx) = sse::channel(128);
            self.client_streams.insert(client_id.clone(), tx);

            // let otkey_count = self.otkeys.get(&client_id).map(|hm| hm.len()).unwrap_or(0);
            // if otkey_count < 10 {
            self.request_otkeys(client_id);
            // }

            DeviceMessageStream(rx)
        }
    }

    impl Handler<GetDeviceMessages> for InboxActor {
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

    // impl Handler<DeleteDeviceMessages> for InboxActor {
    //     type Result = DeviceMessages;

    //     fn handle(&mut self, msg: DeleteDeviceMessages, _ctx: &mut Context<Self>) -> Self::Result {
    //         let (ref mut client_next_epoch, ref mut client_msgs) = self
    //             .client_mailboxes
    //             .entry(msg.0)
    //             .or_insert_with(|| (0, LinkedList::new()));

    //         *client_next_epoch = self.next_epoch;

    //         //FIX: use drain filter if we're ok with experimental
    //         //let leftover_msgs = client_msgs.drain_filter(|x| *x.seq < msg.1).collect::<LinkedList<_>>();

    //         let mut index = 0;
    //         for m in client_msgs.iter().flat_map(|(_, v)| v.iter()) {
    //             if m.seq_id < msg.1 {
    //                 index += 1;
    //             } else {
    //                 break;
    //             }
    //         }

    //         let leftover_msgs = client_msgs.split_off(index);
    //         let msgs = mem::replace(client_msgs, leftover_msgs);

    //         DeviceMessages(
    //             msgs.into_iter()
    //                 .flat_map(|(_, v)| v.into_iter().map(|m| m.clone()))
    //                 .collect(),
    //         )
    //     }
    // }

    impl Handler<ClearDeviceMessages> for InboxActor {
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
async fn inbox_shard(
    state: web::Data<ShardState>,
    auth: web::Header<BearerToken>,
) -> impl Responder {
    let device_id = auth.into_inner().into_token();
    let bucket = hash_into_bucket(&device_id, state.intershard_router_actors.len(), true);

    state.shard_map[bucket].clone()
}

#[derive(Deserialize)]
struct BatchHashQuery {
    pub count: usize,
}


#[get("/shard/batch")]
async fn inbox_shard_batch(
    state: web::Data<ShardState>,
    query: web::Query<BatchHashQuery>,
) -> impl Responder {
    let mut map = HashMap::new();

    for i in 0..(query.count) {
	let device_id = format!("{}", i);
	let bucket = hash_into_bucket(&device_id, state.intershard_router_actors.len(), true);
	map.insert(device_id, state.shard_map[bucket].clone());
    }

    web::Json(map)
}

#[get("/inboxidx")]
async fn inbox_idx(state: web::Data<ShardState>, auth: web::Header<BearerToken>) -> impl Responder {
    let device_id = auth.into_inner().into_token();
    let bucket = hash_into_bucket(&device_id, state.inbox_actors.len(), false);

    format!("{}", bucket)
}

#[get("/inboxidx/batch")]
async fn inbox_idx_batch(
    state: web::Data<ShardState>,
    query: web::Query<BatchHashQuery>,
) -> impl Responder {
    let mut map = HashMap::new();

    for i in 0..(query.count) {
	let device_id = format!("{}", i);
	let bucket = hash_into_bucket(&device_id, state.intershard_router_actors.len(), false);
	map.insert(device_id, state.shard_map[bucket].clone());
    }

    web::Json(map)
}

#[post("/message")]
async fn handle_message(
    msg: web::Json<client_protocol::EncryptedOutboxMessage>,
    state: web::Data<ShardState>,
    auth: web::Header<BearerToken>,
    req: HttpRequest,
) -> impl Responder {
    // println!("Handling message for {:?}: {:?}", auth.token(), msg);

    // Check whether we are the right shard for this client_id:
    let shard_bucket =
        hash_into_bucket(auth.token(), state.intershard_router_actors.len(), true);
    if (state.shard_id as usize) != shard_bucket {
        HttpResponse::TemporaryRedirect()
            .append_header((
                "Location",
                format!(
                    "{}{}?{}",
                    state.shard_map[shard_bucket],
                    req.path(),
                    req.query_string()
                ),
            ))
            .finish()
    } else {
	let sender_id = auth.into_inner().into_token();
	let outbox_actors_cnt = state.outbox_actors.len();
	let actor_idx = hash_into_bucket(&sender_id, outbox_actors_cnt, false);

	// Now, send the message to the corresponding actor:
	let epoch = state.outbox_actors[actor_idx]
            .send(outbox::Event {
		sender: sender_id,
		message: msg.into_inner(),
            })
            .await
            .unwrap()
            .unwrap();

	HttpResponse::Ok().json(epoch)
    }
}

#[get("/events")]
async fn stream_messages(
    state: web::Data<ShardState>,
    auth: web::Header<BearerToken>,
) -> impl Responder {
    // println!("Event listener register request for {:?}", auth.token());

    let device_id = auth.into_inner().into_token();
    let inbox_actors_cnt = state.inbox_actors.len();
    let actor_idx = hash_into_bucket(&device_id, inbox_actors_cnt, false);

    state.inbox_actors[actor_idx]
        .1
        .send(inbox::GetDeviceMessageStream(device_id))
        .await
        .unwrap()
        .0
}

#[delete("/inbox")]
async fn delete_messages(
    state: web::Data<ShardState>,
    auth: web::Header<BearerToken>,
) -> impl Responder {
    let device_id = auth.into_inner().into_token();
    let inbox_actors_cnt = state.inbox_actors.len();
    let actor_idx = hash_into_bucket(&device_id, inbox_actors_cnt, false);

    let messages = state.inbox_actors[actor_idx]
        .1
        .send(inbox::ClearDeviceMessages(device_id))
        .await
        .unwrap();

    web::Json(messages.0)
}

#[delete("/inbox/clear-all")]
async fn clear_all_messages(
    state: web::Data<ShardState>,
) -> impl Responder {
    for (_, ref iba) in state.inbox_actors.iter() {
	iba.send(inbox::ClearAllMessages).await.unwrap()
    }

    "".to_string()
}

// // TODO: find opt param setup and combine with above
// #[delete("/inbox/{seq_high}/{seq_low}")]
// async fn delete_messages(
//     state: web::Data<ShardState>,
//     auth: web::Header<BearerToken>,
//     msg_id: web::Path<(u64, u64)>,
// ) -> impl Responder {
//     let device_id = auth.into_inner().into_token();
//     let inbox_actors_cnt = state.inbox_actors.len();
//     let actor_idx = hash_into_bucket(&device_id, inbox_actors_cnt, false);

//     let messages = state.inbox_actors[actor_idx]
//         .1
//         .send(inbox::DeleteDeviceMessages(device_id, *msg_id))
//         .await
//         .unwrap();

//     web::Json(messages.0)
// }

#[get("/inbox")]
async fn retrieve_messages(
    state: web::Data<ShardState>,
    auth: web::Header<BearerToken>,
) -> impl Responder {
    // println!("Retrieve messages request for {:?}", auth.token());

    let device_id = auth.into_inner().into_token();
    let inbox_actors_cnt = state.inbox_actors.len();
    let actor_idx = hash_into_bucket(&device_id, inbox_actors_cnt, false);

    let messages = state.inbox_actors[actor_idx]
        .1
        .send(inbox::GetDeviceMessages(device_id))
        .await
        .unwrap();

    web::Json(messages.0)
}
#[post("/epoch/{epoch_id}")]
async fn start_epoch(state: web::Data<ShardState>, epoch_id: web::Path<u64>) -> impl Responder {
    // println!("Received start_epoch request: {}", *epoch_id);
    for outbox in state.outbox_actors.iter() {
        outbox.send(outbox::EpochStart(*epoch_id)).await.unwrap();
    }
    ""
}

#[post("/intershard-batch")]
async fn intershard_batch(
    state: web::Data<ShardState>,
    body: web::Bytes,
    // batch: web::Json<intershard::IntershardRoutedEpochBatch>,
) -> impl Responder {
    use web::Buf;
    let batch = bincode::deserialize_from(body.reader()).unwrap();
    intershard::distribute_intershard_batch(&*state, batch);
    ""
}

#[post("/self/otkeys")]
async fn add_otkeys(
    state: web::Data<ShardState>,
    auth: web::Header<BearerToken>,
    keys: web::Json<HashMap<String, String>>,
) -> impl Responder {
    println!(
        "Add otkeys request for {:?} ({} keys)",
        auth.token(),
        keys.len()
    );

    let device_id = auth.into_inner().into_token();
    let inbox_actors_cnt = state.inbox_actors.len();
    let actor_idx = hash_into_bucket(&device_id, inbox_actors_cnt, false);

    state.inbox_actors[actor_idx]
        .1
        .send(inbox::AddOtkeys(device_id, keys.into_inner()))
        .await
        .unwrap();

    HttpResponse::NoContent().finish()
}

#[derive(Deserialize)]
struct GetOtkeyRequestParams {
    pub device_id: String,
}

#[derive(Serialize)]
#[serde(transparent)]
struct GetOtkeyRequestResponse(HashMap<String, String>);

#[get("/devices/otkey")]
async fn get_otkey(
    state: web::Data<ShardState>,
    query: web::Query<GetOtkeyRequestParams>,
    req: HttpRequest,
) -> impl Responder {
    println!("Get otkey request for {:?}", &query.device_id);

    // Check whether we are the right shard for this client_id:
    let shard_bucket =
        hash_into_bucket(&query.device_id, state.intershard_router_actors.len(), true);
    if (state.shard_id as usize) != shard_bucket {
        HttpResponse::TemporaryRedirect()
            .append_header((
                "Location",
                format!(
                    "{}{}?{}",
                    state.shard_map[shard_bucket],
                    req.path(),
                    req.query_string()
                ),
            ))
            .finish()
    } else {
        let inbox_actors_cnt = state.inbox_actors.len();
        let actor_idx = hash_into_bucket(&query.device_id, inbox_actors_cnt, false);

        let opt_otkey = state.inbox_actors[actor_idx]
            .1
            .send(inbox::GetOtkey(query.into_inner().device_id))
            .await
            .unwrap();

        if let Some((_k, v)) = opt_otkey.0 {
            let mut resp_map = HashMap::new();
            resp_map.insert("otkey".to_string(), v);
            HttpResponse::Ok().json(GetOtkeyRequestResponse(resp_map))
        } else {
            println!("Did not have the requested otkey");
            HttpResponse::NotFound().finish()
        }
    }
}

pub struct ShardState {
    httpc: reqwest::Client,
    shard_id: u8,
    shard_map: Vec<String>,
    sequencer_url: String,
    outbox_actors: Vec<Addr<outbox::OutboxActor>>,
    intershard_router_actors: Vec<Addr<intershard::InterShardRouterActor>>,
    inbox_actors: Vec<(Addr<inbox::ReceiverActor>, Addr<inbox::InboxActor>)>,
    epoch_collector_actor: Addr<intershard::EpochCollectorActor>,
    attestation_key: ed25519_dalek::Keypair,
}

pub async fn init(
    shard_base_url: String,
    sequencer_base_url: String,
    outbox_count: u8,
    inbox_count: u8,
) -> impl Fn(&mut web::ServiceConfig) + Clone + Send + 'static {
    use ed25519_dalek::Keypair;
    use rand::rngs::OsRng;
    use std::fs::{self, File, OpenOptions};
    use std::io::{self, Read, Write};
    use std::mem;
    use std::path::Path;
    use tokio::sync::mpsc;

    // Read server attestation private key or generate a new one
    let keypair_path = Path::new("./attestation-key");
    let keypair = if let Ok(mut keypair_file) = File::open(keypair_path) {
        let mut keypair_bytes = [0; 64];
        assert!(keypair_file.read(&mut keypair_bytes).unwrap() == 64);
        Keypair::from_bytes(&keypair_bytes).unwrap()
    } else {
        let mut csprng = OsRng {};
        let keypair = Keypair::generate(&mut csprng);
        let mut keypair_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(keypair_path)
            .unwrap();
        keypair_file.write(&keypair.to_bytes()).unwrap();
        keypair_file.flush().unwrap();
        mem::drop(keypair_file);
        keypair
    };

    {
        use base64::{engine::general_purpose, Engine as _};
        let encoded = general_purpose::STANDARD_NO_PAD.encode(&keypair.public.to_bytes());
        println!("Loaded attestation key, public key: {}", encoded);
    }

    fs::create_dir("./persist-outbox").unwrap();

    let httpc = reqwest::Client::new();
    let mut arbiters = Vec::new();

    // Register ourselves with the sequencer to be assigned a shard id
    println!(
        "Registering ourselves ({}) at sequencer ({}) with {}/{} outboxes/inboxes...",
        shard_base_url, sequencer_base_url, outbox_count, inbox_count
    );

    let register_resp = httpc
        .post(format!("{}/register", &sequencer_base_url))
        .json(&crate::sequencer::SequencerRegisterReq {
            base_url: shard_base_url,
            outbox_count,
            inbox_count,
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

    // Boot up a set of outbox actors on individual arbiters
    let mut outbox_actors: Vec<Addr<outbox::OutboxActor>> = Vec::new();
    for id in 0..outbox_count {
        let (router_tx, mut router_rx) = mpsc::channel(1);
        let router_arbiter = Arbiter::new();
        assert!(router_arbiter.spawn(async move {
            let addr = outbox::RouterActor::new(id).start();
            router_tx.send(addr).await.unwrap();
        }));
        arbiters.push(router_arbiter);
        let router = router_rx.recv().await.unwrap();

        let (outbox_tx, mut outbox_rx) = mpsc::channel(1);
        let outbox_arbiter = Arbiter::new();
        assert!(outbox_arbiter.spawn(async move {
            let addr = outbox::OutboxActor::new(id, router).start();
            outbox_tx.send(addr).await.unwrap();
        }));
        arbiters.push(outbox_arbiter);

        outbox_actors.push(outbox_rx.recv().await.unwrap());
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

    // Boot up a set of inbox actors on individual arbiters:
    let mut inbox_actors: Vec<(Addr<inbox::ReceiverActor>, Addr<inbox::InboxActor>)> = Vec::new();
    for id in 0..inbox_count {
        let (inbox_tx, mut inbox_rx) = mpsc::channel(1);
        let inbox_arbiter = Arbiter::new();
        let _sequencer_clone = sequencer_base_url.clone();
        assert!(inbox_arbiter.spawn(async move {
            let addr = inbox::InboxActor::new(id).start();
            inbox_tx.send(addr).await.unwrap();
        }));
        arbiters.push(inbox_arbiter);
        let inbox_addr = inbox_rx.recv().await.unwrap();

        let (recv_tx, mut recv_rx) = mpsc::channel(1);
        let recv_arbiter = Arbiter::new();
        let inbox_addr_clone = inbox_addr.clone();
        assert!(recv_arbiter.spawn(async move {
            let addr = inbox::ReceiverActor::new(id, inbox_addr_clone).start();
            recv_tx.send(addr).await.unwrap();
        }));
        arbiters.push(recv_arbiter);

        inbox_actors.push((recv_rx.recv().await.unwrap(), inbox_addr));
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
        outbox_actors,
        intershard_router_actors,
        inbox_actors,
        epoch_collector_actor: epoch_collector_actor.clone(),
        attestation_key: keypair,
    });

    for outbox_actor in state.outbox_actors.iter() {
        outbox_actor
            .send(outbox::Initialize(state.clone().into_inner()))
            .await
            .unwrap();
    }

    for intershard_router_actor in state.intershard_router_actors.iter() {
        intershard_router_actor
            .send(intershard::Initialize(state.clone().into_inner()))
            .await
            .unwrap();
    }

    for inbox_actor in state.inbox_actors.iter() {
        inbox_actor
            .0
            .send(inbox::Initialize(state.clone().into_inner()))
            .await
            .unwrap();
        inbox_actor
            .1
            .send(inbox::Initialize(state.clone().into_inner()))
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
            .app_data(web::PayloadConfig::new(usize::MAX))
            .app_data(web::JsonConfig::default().limit(usize::MAX))
            // Client API
            .service(handle_message)
            .service(retrieve_messages)
            // .service(delete_messages)
	    .service(delete_messages)
            .service(clear_all_messages)
            .service(stream_messages)
            .service(inbox_shard)
            .service(inbox_idx)
	    .service(inbox_shard_batch)
            .service(inbox_idx_batch)
            .service(get_otkey)
            .service(add_otkeys)
            // Sequencer API
            .service(start_epoch)
            .service(index)
            // Intershard API
            .service(intershard_batch);
    })
}
