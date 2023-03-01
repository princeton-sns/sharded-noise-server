use serde_derive::{Deserialize, Serialize};
use std::net::SocketAddr;
use pickledb::{PickleDb, PickleDbDumpPolicy, SerializationMethod};
// use pickledb::error::{Error, ErrorType};
use warp::{Filter, /*Rejection, Reply,*/ http::header::HeaderValue};
use core::convert::Infallible;
use std::sync::{Arc, Mutex};

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

pub struct Store {
    seq : u64,
    db: PickleDb,
}

impl Store {
    fn init(db_path: &str) -> Self {
        let db = PickleDb::new(
            db_path,
            PickleDbDumpPolicy::AutoDump,
            SerializationMethod::Json,
        );
        Store { seq: 0, db: db }
    }

    // FIXME : return error so that it can be formatted in response
    fn add_message(&mut self, device_id: String, message: &OutgoingMessage) -> Result<(), pickledb::error::Error> {

        if !self.db.lexists(&device_id) {
            self.db.lcreate(&device_id).unwrap().ladd(message);
            Ok(())
        } else {
            self.db.ladd(&device_id, message);
            Ok(())
        }
    }

    fn up(&mut self) -> u64 {
        self.seq += 1;
        return self.seq
    }
}

pub fn add<T>(s: T) -> impl Filter<Extract = (T,), Error = std::convert::Infallible> + Clone
where
  T: Clone + Send,
{
  warp::any().map(move || s.clone())
}


pub async fn post_message(
    header: HeaderValue,
    store: Arc<Mutex<Store>>,
    batch: Batch,
) -> Result<impl warp::Reply, Infallible> {

    //process header
    let sender_id = header.to_str().unwrap();
    // let batch = warp::body::json()::<Batch>;

    let mut s = store.lock().unwrap();
    for message in batch.messages.iter() {
        let m = OutgoingMessage{
            sender: sender_id.to_string(),
            payload: message.payload.clone(),
            seq_id: Store::up(&mut s), 
        };
        Store::add_message(&mut s, message.device_id.clone(), &m);
    };

    Ok(format!("Message added from : {:?}", sender_id))
}

#[tokio::main]
async fn main() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    let store_arc = Arc::new(Mutex::new(Store::init("noise.db")));

    // Define routes for handling messages
    let post_message_route = warp::path("message")
        .and(warp::post())
        .and(warp::header::value("authentication"))
        .and(add(store_arc))
        .and(warp::body::json())
        .and_then(post_message);

    // let get_messages_route = warp::path!("self/messages")
    //     .and(warp::get())
    //     .and_then(get_messages);

    // let delete_messages_route = warp::path!("self/messages")
    //     .and(warp::delete())
    //     .and_then(delete_messages);

    // let routes = post_message_route.and(get_messages_route).and(delete_messages_route);

    warp::serve(post_message_route).run(addr).await
}

#[tokio::test]
async fn test_message_post() {
    unimplemented!();
}
