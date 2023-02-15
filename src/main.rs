use warp::Filter;

mod models {
    use serde_derive::{Deserialize, Serialize};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    // TODO: make persistent
    pub type Db = Arc<Mutex<Vec<Todo>>>;

    pub fn blank_db() -> Db {
        Arc::new(Mutex::new(Vec::new()))
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct IncomingMessage {
        device_id: String,
        payload: String,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct OutgoingMessage {
        sender: String,
        payload: String,
        seq_id: u64,
    }

    struct Message {
        to: String,
        outgoing: OutgoingMessage,
    }
}

mod filters {
    use super::handlers;
    use super::models::{Db, IncomingMessage, OutgoingMessage, Message};
    use warp::Filter;

    // handle /message
    pub fn message(db:Db) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        message_post(db);
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
            .or(messages_delete(db.clone())
    }

    // GET /self/messages
    pub fn messages_get() -> Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        unimplemented!();
    }

    // DELETE /self/messages
    pub fn messages_delete() -> Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        unimplemented!();
    }

}

mod handlers {
    fn post_message(&self, rw: &str, req: &str) {
        unimplemented!();
    }

    fn get_messages(&self, rw: &str, req: &str) {
        unimplemented!();
    }

    fn delete_messages(&self, rw: &str, req: &str) {
        unimplemented!();
    }

    fn get_one_time_key(&self, rw: &str, req: &str) {
        unimplemented!();
    }

    fn add_one_time_keys(&self, rw: &str, req: &str) {
        unimplemented!();
    }

}


#[tokio::main]
async fn main() {
    let db = models::blank_db();
    let routes = filters::message(db).with(filters::messages(db));
    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
}
