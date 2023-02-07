extern crate bytes;
extern crate encoding_rs;
extern crate pebble;
use reqwest::{Response, Error};
use reqwest::header::{HeaderMap, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS};

use pebble::DB;
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::thread;
use serde::{Serialize, Deserialize};

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

struct ClientChan {
    device_id: String,
    channel: String,
}

struct MessageStorage {
    lock: Mutex<()>,
    db: Arc<DB>,
}

struct Server {
    message_storage: MessageStorage,
    notifier: Arc<Mutex<()>>,
    new_clients: Arc<Mutex<()>>,
    closing_clients: Arc<Mutex<()>>,
    clients: Arc<Mutex<HashMap<String, String>>>,
}

impl Server {
    fn new(db: Arc<DB>) -> Server {
        let server = Server {
            message_storage: MessageStorage {
                lock: Mutex::new(()),
                db,
            },
            notifier: Arc::new(Mutex::new(())),
            new_clients: Arc::new(Mutex::new(())),
            closing_clients: Arc::new(Mutex::new(())),
            clients: Arc::new(Mutex::new(HashMap::new())),
        };
        thread::spawn(move || {
            server.listen();
        });
        server
    }

    fn listen(&self) {
        unimplemented!();
    }

    fn serve_events(&self, rw: &str, req: &str) {
        unimplemented!();
    }

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

async fn handle_request(req: Request) -> Result<Response, Error> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, "*".parse().unwrap());
    headers.insert(ACCESS_CONTROL_ALLOW_HEADERS, "*".parse().unwrap());
    headers.insert(ACCESS_CONTROL_ALLOW_METHODS, "OPTIONS,GET,POST,DELETE".parse().unwrap());

    let method = req.method().to_owned();
    let url = req.url().to_owned();

    match url.path() {
        "/events" => {
            if method != "GET" {
                return Ok(Response::builder().status(404).body("Not Found".into()).unwrap());
            }
            // Subscribe to server-sent events
            //server.serve_events(rw, req)
        },
        "/message" => {
            if method != "POST" {
                return Ok(Response::builder().status(404).body("Not Found".into()).unwrap());
            }
            // Post a message
            //server.post_message(rw, req)
        },
        "/self/messages" => {
            if method == "GET" {
                // Retrieve outstanding messages
                //server.get_messages(rw, req)
            } else if method == "DELETE" {
                // Delete processed messages
                //server.delete_messages(rw, req)
            } else {
                return Ok(Response::builder().status(404).body("Not Found".into()).unwrap());
            }
        },
        "/devices/otkey" => {
            if method != "GET" {
                return Ok(Response::builder().status(404).body("Not Found".into()).unwrap());
            }
            // Get one-time key for a device
            //server.get_one_time_key(rw, req)
        },
        "/self/otkeys" => {
            if method != "POST" {
                return Ok(Response::builder().status(404).body("Not Found".into()).unwrap());
            }
            // Add new one-time keys for this device
            //server.add_one_time_keys(rw, req)
        },
        _ => {
            return Ok(Response::builder().status(404).body("Not Found".into()).unwrap());
        }
    }
    Ok(Response::builder().status(200).body("".into()).unwrap())
}

fn main() {
    unimplemented!();
}

#[cfg(test)]
mod tests {
  use crate::core::{Core, FullPayload};
  use crate::server_comm::{Event, IncomingMessage, ToDelete};
  use futures::TryStreamExt;
  use futures::channel::mpsc;

  const BUFFER_SIZE: usize = 20;
  
  //#[tokio::test]
  //asynch fn test_new() {
  //  unimplemented!();
  //}
}


