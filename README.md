# Scaling the Noise Server

## General design Questions

* Is there a way to parallelize across shards better than using a sharded db?
* Who is the reordering "cheater" for a sharded db? the entire db or a shard?
* What does "reordering" mean if server on top of serializable txn db?
* How can we design this in a fully decentralized way? think mastodon active record 
but for timely routing instead of actual information storage. 
    * translate distributed db model so that clients are aware of shard their mailbox belongs to
    * migrate mailbox at will across instances or providers consistently
* How can we divide the blame of misbehavior across multiple server instances? 

## Central

A centralized server mimicking our go implementation but in Rust.

[TODO]: implement notifications to clients

### Impl

Server is setup using *warp* framework which applies filters to requests.

Running on top of *PickleDB* because it has "lists" to alleviate overheads of de/serializing and 
getting and then setting mailbox contents on every append in a traditional key-value store.

There is a single lock over the "data store" type. Once lock is grabbed, the sequence id is incremented
and then newly generated outgoing messages are appended to a client's outgoing queue.

To address TODO I plan on using Tower (there is tower <-> warp compatibility) to facilicate communication
over channels with clients.

## [TODO] Multithreaded

A multithreaded version of central server.

## [In progress] Parallel Server

Use actix/erlang.

Cool: everyone interacts with the same server to send messages, but has specific
instance to receive messages from.

Not cool: 

### Design

**Request handler**

Handles requests directly from client and load balances messages to inbox actors 
based on the sender id.

**Inbox**

Actor that receives messages from request handlers and fills a queue within a single epoch.
When the epoch is started, the message queue is transferred to the router and started over.

**Router**

The router transforms the message queue into a hash map of vectors based on receivers.
For each message in the message queue, the router breaks down the message bundle
adds the inbox index to all messages in a bundle, and puts each message into a vector destined for the receiver
responsible for the mailbox for the destination client.

**Receiver**

A receiver processes messages from one router at a time. The receiver puts each message into
a vector based on the destination client. Each receiver iterates over routers in the same order.

**Outbox**

An outbox holds all messages destined for a client from previous epochs.

**Sequencer**

A sequencer communicates with inboxes/routers to indicate epoch starts.
All outboxes communicate with the sequencer when they receive their list of messages
for an epoch.
Once a sequencer receives indication from all outboxes that an epoch has been committed, it 
sends a new epoch signal to the inboxes.

### Client-facing Attestation of correctness

Outgoing message actors will need to sign over the messages received for a mailbox in an epoch.
 * Global per-message seq number
 * hash of the recipients (produced by the sending client)
 * symetric message content
 * Epoch number
 * Actor id

Server can do this :
   * drop a message to a rec
   * change seq number
   * change event contents (rec list or actual message text) 

If a client catches a reordering, it complains to its mailbox, i.e. outgoing actor. 
Reordering means:
   * [CASE 1] One client did not receive a message it should have with seq number n
   * [CASE 2] One client has a different recipient list or message payload for a message with seq number n

A client needs to store the following info:
   * All epochs with any unverified message containing all messages in that epoch
      * option 1: garbage collect after k epochs
      * option 2: verify epochs across all connections
      * option 3: compress epoch and save indefinetly, compress by including attestations over prior events/epochs  (server saves previous attestations, and include smaller ones in the future up to some limit)
   * Per message, also saving:
      * seq number
      * hash of the recipients (produced by the sending client)
      * symetric message content
   
If a client sees a divergence in hash chains, it does the following out of band:
   * send unverified section of hash vector
   * reciprocate
   * when a client has both vectors :
         * walk back, find sequence number where divergence occurred
         * send attestation over that message or nonexistance
         * client compares with local attestation and can produce proof
   * server sad

### Internal Attestation

Each actor in the system will sign over the message before forwarding it to the
next actor.
Incoming message actor needs to sign over message bundle, the epoch number, and
the index of the bundle in its queue.
They also have to attest to an epoch if there are no messages in it.

### Other
The outgoing actor can "accept blame" or find out which incoming actor is responsible for the error.
The mailbox can then pinpoint where the reordering occured in the system by blaming other actors and attesting to it.

Clients can request their mailbox be transferred to a different actor or handled by a different incoming message actor.





