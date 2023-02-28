use serde_derive::{Deserialize, Serialize};
use std::net::SocketAddr;
use pickledb::{PickleDb, PickleDbDumpPolicy, SerializationMethod};
use warp::{Filter, Rejection, Reply};
// use warp::header::{HeaderMap, HeaderValue, AUTHORIZATION};

#[derive(Serialize, Deserialize, Debug)]
pub struct Batch{
    messages: Vec<IncomingMessage>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct IncomingMessage {
    device_id: String,
    payload: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OutgoingMessage {
    sender: String,
    payload: String,
    seq_id: u64,
}

pub struct Message {
    to: String,
    outgoing: OutgoingMessage,
}

struct Store {
    db: PickleDb,
}

impl Store {
    fn new(db_path: &str) -> Self {
        let db = PickleDb::new(
            db_path,
            PickleDbDumpPolicy::AutoDump,
            SerializationMethod::Json,
        );
        Store { db }
    }

    fn add_message(&mut self, device_id: String, message: &OutgoingMessage) -> Result<(), pickledb::error::Error> {
        // add locks on this (or txn in pickle?)
        let res;
        if !self.db.lexists(&device_id) {
            res = self.db.lcreate(&device_id).unwrap().ladd(message);
            Ok(())
        } else {
            res = self.db.ladd(&device_id, message);
            Ok(())
        }
    }
}

async fn post_message(
    batch: Batch,
    store: Store,
) -> Result<impl Reply, Rejection> {

    //get sender id
    let sender_id = warp::header::<String>("authorization");

    //unbatch and puth messages
    // let batch = warp::body()::<Batch>("batch");
    for message in batch.messages.iter() {
        let mut m = OutgoingMessage{
            sender: sender_id,
            payload: message.payload,
            seq_id: 0, //FIXME: impl this
        };
        store.add_message(message.device_id, &m);
    }
    Ok(format!("Message added from : {:?}", sender_id))
}

#[tokio::main]
async fn main() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));

    let mut store = Store::new("noise.db");

    // Define routes for handling messages
    let post_message_route = warp::path("message")
        .and(warp::post())
        .and(warp::header::headers_cloned())
        .and(warp::body::json())
        .and_then(post_message);

    // let get_messages_route = warp::path!("self/messages")
    //     .and(warp::get())
    //     .and_then(get_messages);

    // let delete_messages_route = warp::path!("self/messages")
    //     .and(warp::delete())
    //     .and_then(delete_messages);

    // let routes = post_message_route.and(get_messages_route).and(delete_messages_route);

    warp::serve(post_message_route).run(([127, 0, 0, 1], 3030)).await
}

#[tokio::test]
async fn test_message_post() {
    unimplemented!();
}
