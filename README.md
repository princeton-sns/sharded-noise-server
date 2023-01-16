# Sharded Server

sharded or federated? is it the same in this case since it's not necessarily a db?

## Design

* How does routing change?
	* Now tracking both mailbox id and shard number
	* what if a user has multiple mailboxes but they're on different shards?
* Cross-shard?
	* How much information do shards know about each other?
	* Is there any cross-shard communication for a simple put?
* Load/mailbox balancing?
	* Do we need a coordinator?
	* Can we move a mailbox from one shard to another if its a better fit?
	* How do we track off-shard accesses (for example, this mailbox overlaps with X mailbox hits on shard 1, Y on shard 2? can we move it to shard X to minimize off-shard stuff?
* How does shard info not leak information about mailboxes in it?


## Implementation

## 
