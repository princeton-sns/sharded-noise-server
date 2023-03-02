use serde_derive::{Deserialize, Serialize};
use std::net::SocketAddr;
use pickledb::{PickleDb, PickleDbDumpPolicy, SerializationMethod};
// use pickledb::error::{Error, ErrorType};
use warp::{Filter, /*Rejection, Reply,*/ http::header::HeaderValue};
use core::convert::Infallible;
use tokio::sync::Mutex;
use std::sync::Arc;
use std::result::Result;

#[derive(Serialize, Deserialize, Debug)]
pub struct Batch{
    messages: Vec<IncomingMessage>,
}

impl Batch {
    pub fn from_vec(batch: Vec<IncomingMessage>) -> Self {
        Self { messages: batch }
      }
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

#[derive(Debug, Serialize, Deserialize)]
pub struct OtkeyResponse {
  otkey: String,
}

impl From<OtkeyResponse> for String {
  fn from(otkey_response: OtkeyResponse) -> String {
    otkey_response.otkey
  }
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

    fn up(&mut self) -> u64 {
        self.seq += 1;
        return self.seq
    }

    fn add_message(&mut self, device_id: String, message: &OutgoingMessage) -> Result<String, String>{

        if self.db.lexists(&device_id) {
            let ret = self.db.ladd(&device_id, message);
            if ret.is_none(){
                return Err(device_id);
            }
        } else {
            let ret = self.db.lcreate(&device_id);
            if ret.is_err(){
                return Err(device_id);
            }
            let ret2 = self.db.ladd(&device_id, message);
            if ret2.is_none(){
                return Err(device_id);
            }
        }
        Ok("post".to_string())
    }

    fn get_mailbox(&mut self, device_id: String,) -> Result<Vec<OutgoingMessage>, String> {
        let mut messages : Vec<OutgoingMessage> = Vec::new();
        for m in self.db.liter(&device_id) {
            messages.push(m.get_item::<OutgoingMessage>().unwrap())
        }
        Ok(messages)
    }

    fn empty_mailbox(&mut self, device_id: String, seq_number: u64) -> Result<String, String>{

        //note : can't remove while iterating over
        let mut stale_messages : Vec<OutgoingMessage> = Vec::new();
        for m in self.db.liter(&device_id) {
            let message = m.get_item::<OutgoingMessage>().unwrap();
            if message.seq_id <= seq_number {
                stale_messages.push(message)
            }
        }

        for message in stale_messages {
            self.db.lrem_value(&device_id, &message);
        }
        Ok("delete".to_string())
    }

}

/**
 * Allows adding parameters to warp filter commands
 */
pub fn add<T>(s: T) -> impl Filter<Extract = (T,), Error = std::convert::Infallible> + Clone 
where T: Clone + Send,
{
  warp::any().map(move || s.clone())
}

pub async fn post_message(
    header: HeaderValue,
    store: Arc<Mutex<Store>>,
    batch: Batch,
) -> Result<impl warp::Reply, Infallible> {

    let sender_id = header.to_str().unwrap();
    //put lock call here so login of converting messages is not done within "store" functions
    let mut s = store.lock().await;

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

pub async fn delete_messages(
    header: HeaderValue,
    store: Arc<Mutex<Store>>,
    seq: u64,
) -> Result<impl warp::Reply, Infallible> {

    let sender_id = header.to_str().unwrap();
    let mut s = store.lock().await;

    Store::empty_mailbox(&mut s, sender_id.to_string(), seq);

    Ok(format!("Messages removed from : {:?} up to : {:?}", sender_id, seq))
}

pub async fn get_messages(
    header: HeaderValue,
    store: Arc<Mutex<Store>>
) -> Result<impl warp::Reply, Infallible> {
    let sender_id = header.to_str().unwrap();
    let mut s = store.lock().await;

    let messages = Store::get_mailbox(&mut s, sender_id.to_string());
    Ok(warp::reply::json(&messages))
}

pub fn handlers (store_arc: Arc<Mutex<Store>>) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone{
    let post_message_route = warp::path("message")
        .and(warp::post())
        .and(warp::header::value("authentication"))
        .and(add(store_arc.clone()))
        .and(warp::body::json())
        .and_then(post_message);

    let get_messages_route = warp::path("self/messages")
        .and(warp::get())
        .and(warp::header::value("authentication"))
        .and(add(store_arc.clone()))
        .and_then(get_messages);

    let delete_messages_route = warp::path("self/messages")
        .and(warp::delete())
        .and(warp::header::value("authentication"))
        .and(add(store_arc))
        .and(warp::body::json())
        .and_then(delete_messages);

    return post_message_route.or(get_messages_route).or(delete_messages_route)
}

#[tokio::main]
async fn main() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    let store_arc = Arc::new(Mutex::new(Store::init("noise.db")));

    let routes = handlers(store_arc);
    warp::serve(routes).run(addr).await
}

