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

## [In progress] Parallel

Use actix/erlang.

Cool: everyone interacts with the same server to send messages, but has own
instance to receive messages from.

Not cool: 


### Design

**Request handler**

Handles requests and assigns messages to incoming message actors based on the 
sender.

**Incoming message actors**

Two actos that flip states between receiving messages and sending messages.
The sending actors forward messages to the mailboxes.

**Sequencer**

**Mailbox**

### Verifying correct behavior

Each actor in the system will sign over the message before forwarding it to the
next actor.
Incoming message actor needs to sign over message bundle, the epoch number, and
the index of the bundle in its queue.
Outgoing message actors will need to sign over the incoming signature, the message itself 
(after the bundle is distributed), as well as the sequence id and a per-client sequence number.

If a client catches a reordering, it complains to its mailbox, i.e. outgoing actor. The outgoing
actor can "accept blame" or find out which incoming actor is responsible for the error.
The mailbox can then pinpoint where the reordering occured in the system.

Clients can request their mailbox be transferred to a different actor or handled
by a different incoming message actor.
