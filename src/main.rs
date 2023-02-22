use warp::Filter;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

struct Server<T> {
    message_storage: Arc<Mutex<HashMap<String, Vec<T>>>>,
    clients: Arc<Mutex<HashMap<String, String>>>,
    // notifier: Arc<Mutex<()>>,
    // new_clients: Arc<Mutex<()>>,
    // closing_clients: Arc<Mutex<()>>,
}

impl<T> Server<T>{
    fn new() -> Server<T> {
        let server = Server {
            message_storage: Arc::new(Mutex::new(HashMap::new())),
            clients: Arc::new(Mutex::new(HashMap::new())),
            // notifier: Arc::new(Mutex::new(Vec::new())),
            // new_clients: Arc::new(Mutex::new(Vec::new())),
            // closing_clients: Arc::new(Mutex::new(Vec::new())),
        };
        server
    }
}

mod models {
    use serde_derive::{Deserialize, Serialize};
    use std::sync::{Arc, Mutex};

    pub type Db = Arc<Mutex<Vec<Mailbox>>>;

    pub fn blank_db() -> Db {
        Arc::new(Mutex::new(Vec::new()))
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

    #[derive(Serialize, Deserialize, Debug)]
    pub struct Mailbox {
        owner: String,
        messages: Vec<OutgoingMessage>
    }
}

mod filters {
    use super::handlers;
    use super::models::{Db, IncomingMessage, OutgoingMessage, Message};
    use warp::Filter;

    // handle /message
    pub fn message(db:Db) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        message_post(db)
    }

    // POST /message
    pub fn message_post(
        db: Db,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("message")
            .and(warp::post())
            .and(with_db(db))
            .and_then(handlers::post_message)
    }

    // handle /self/messages
    pub fn messages(
        db: Db,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        messages_get(db.clone())
            .or(messages_delete(db.clone()))
    }

    // GET /self/messages
    pub fn messages_get(
        db: Db,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        unimplemented!();
    }

    // DELETE /self/messages
    pub fn messages_delete(
        db: Db,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        unimplemented!();
    }

    fn with_db(db: Db) -> impl Filter<Extract = (Db,), Error = std::convert::Infallible> + Clone {
        warp::any().map(move || db.clone())
    }

}

mod handlers {
    pub fn post_message(rw: &str, req: &str) {
        unimplemented!();
    }

    pub fn get_messages(rw: &str, req: &str) {
        unimplemented!();
    }

    pub fn delete_messages(rw: &str, req: &str) {
        unimplemented!();
    }

    pub fn get_one_time_key(rw: &str, req: &str) {
        unimplemented!();
    }

    pub fn add_one_time_keys(rw: &str, req: &str) {
        unimplemented!();
    }

}


#[tokio::main]
async fn main() {
    let db = models::blank_db();
    let routes = filters::message(db).with(filters::messages(db));
    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
}
