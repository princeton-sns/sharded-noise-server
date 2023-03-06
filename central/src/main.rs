use serde_derive::{Deserialize, Serialize};
use std::net::SocketAddr;
use pickledb::{PickleDb, PickleDbDumpPolicy, SerializationMethod};
// use pickledb::error::{Error, ErrorType};
use warp::{Filter, /*Rejection, Reply,*/ http::header::HeaderValue};
use core::convert::Infallible;
use tokio::sync::Mutex;
use std::sync::Arc;
use std::result::Result;
use std::collections::HashMap;

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

pub struct MessageStorage {
    seq : u64,
    db: PickleDb,
}

impl MessageStorage {
    fn new(db_path: &str) -> Self {
        let db = PickleDb::new(
            db_path,
            PickleDbDumpPolicy::AutoDump,
            SerializationMethod::Json,
        );
        MessageStorage { seq: 0, db: db }
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

        //note : can't remove while iterating
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

    fn get(&mut self, key: String) -> String {
        let key =  self.db.get::<String>(&key).unwrap();
        return key;
    }

    fn set(&mut self, key: String, value: String) -> Result<String, String>{
        
        self.db.set(&key, &value);
        Ok("otkeys set".to_string())
    }

}

pub struct Server {
    storage: MessageStorage,
    
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
    store: Arc<Mutex<MessageStorage>>,
    batch: Batch,
) -> Result<impl warp::Reply, Infallible> {

    let sender_id = header.to_str().unwrap();
    //put lock call here so login of converting messages is not done within "store" functions

    let mut s = store.lock().await;

    let seq = MessageStorage::up(&mut s);
    for message in batch.messages.iter() {
        let m = OutgoingMessage{
            sender: sender_id.to_string(),
            payload: message.payload.clone(),
            seq_id: seq, 
        };
        MessageStorage::add_message(&mut s, message.device_id.clone(), &m);
    };
    Ok(format!("Message added from : {:?}", sender_id))
}

pub async fn delete_messages(
    header: HeaderValue,
    store: Arc<Mutex<MessageStorage>>,
    seq: u64,
) -> Result<impl warp::Reply, Infallible> {

    let sender_id = header.to_str().unwrap();
    let mut s = store.lock().await;

    MessageStorage::empty_mailbox(&mut s, sender_id.to_string(), seq);

    Ok(format!("Messages removed from : {:?} up to : {:?}", sender_id, seq))
}

pub async fn get_messages(
    header: HeaderValue,
    store: Arc<Mutex<MessageStorage>>
) -> Result<impl warp::Reply, Infallible> {
    let sender_id = header.to_str().unwrap();
    let mut s = store.lock().await;

    let messages = MessageStorage::get_mailbox(&mut s, sender_id.to_string());
    Ok(warp::reply::json(&messages))
}

pub async fn get_one_time_key(
    param: String,
    store: Arc<Mutex<MessageStorage>>,
    device_id: String,
)-> Result<impl warp::Reply, Infallible> {

    // doesn't necessarily need to lock store? can occur concurrenly to message posting if pickle allows
    let mut s = store.lock().await;
    let key = "otkey/".to_owned() + &device_id + &"/".to_owned() + &param;
    let value = MessageStorage::get(&mut s, key);
    Ok(warp::reply::json(&value))
}

pub async fn set_one_time_keys(
    header: HeaderValue,
    store: Arc<Mutex<MessageStorage>>,
    keys: HashMap<String, String>,
)-> Result<impl warp::Reply, Infallible> {

    let sender_id = header.to_str().unwrap();
    let mut s = store.lock().await;

    for (key, value) in &keys{
        s.set(key.to_string() + &"/".to_owned() + &sender_id.to_string() , value.to_string() );
    }
    Ok(warp::reply::json(&keys))
}

pub fn handlers (store_arc: Arc<Mutex<MessageStorage>>) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone{

    let post_message_route = warp::path("message")
        .and(warp::post())
        .and(warp::header::value("Authentication"))
        .and(add(store_arc.clone()))
        .and(warp::body::json())
        .and_then(post_message);

    let get_messages_route = warp::path!("self" / "messages")
        .and(warp::get())
        .and(warp::header::value("Authentication"))
        .and(add(store_arc.clone()))
        .and_then(get_messages);

    let delete_messages_route = warp::path!("self" / "messages")
        .and(warp::delete())
        .and(warp::header::value("Authentication"))
        .and(add(store_arc.clone()))
        .and(warp::body::json())
        .and_then(delete_messages);
    
    let get_one_time_key_route = warp::path!("devices" / "otkey")
        .and(warp::get())
        .and(warp::query::<String>())
        .and(add(store_arc.clone()))
        .and(warp::body::json())
        .and_then(get_one_time_key);

    let post_one_time_keys_route = warp::path!("self" / "otkeys")
        .and(warp::post())
        .and(warp::header::value("Authentication"))
        .and(add(store_arc))
        .and(warp::body::json())
        .and_then(set_one_time_keys);

    //  let events_route = warp::path("events")
    //     .and(warp::get())
    //     .and(add(store_arc.clone()))
    //     .and_then()

    return post_message_route
        .or(get_messages_route)
        .or(delete_messages_route)
        .or(get_one_time_key_route)
        .or(post_one_time_keys_route);
}

#[tokio::main]
async fn main() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let store_arc = Arc::new(Mutex::new(MessageStorage::new("noise.db")));

    let routes = handlers(store_arc);
    warp::serve(routes).run(addr).await
}
