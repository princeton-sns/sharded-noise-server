/**
 * Notes: there must be better ways to do half of these things. Things to fix:
 * - have multiple transactions in a scenario over the user username in the system
 * - reuse request generators in complex patterns like deletepostdelete
 * - make username generation based on number of users
 */
use goose::prelude::*;
use goose_eggs::{validate_and_load_static_assets, Validate};
use rand::seq::SliceRandom;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::Serialize;
use serde_json::{json, Value};

// static LETTERS: [&str; 26] = [
//     "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s",
//     "t", "u", "v", "w", "x", "y", "z",
// ];
// static NUMBERS: [&str; 10] = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"];

// Horribly unsafe AND unsound, never do this, this WILL break, it's terrible.
static mut USERNAMES: Option<Vec<String>> = None;
static mut FRIENDS: Option<Vec<String>> = None;

#[tokio::main]
async fn main() -> Result<(), GooseError> {
    let g = GooseAttack::initialize()?;

    let usernames_str = std::env::vars()
        .find(|(var, _)| var == "NOISE_USERS")
        .map(|(_, val)| val)
        .unwrap();
    let usernames = usernames_str.split(":").map(|s| s.to_string()).collect();
    unsafe { USERNAMES = Some(usernames) };

    let friends_str = std::env::vars()
        .find(|(var, _)| var == "FRIENDS")
        .map(|(_, val)| val)
        .unwrap();
    let friends: Vec<String> = friends_str.split(":").map(|s| s.to_string()).collect();
    unsafe { FRIENDS = Some(friends) }

    g.register_scenario(
        scenario!("PostToOneUserAndSelf")
            .register_transaction(transaction!(set_username).set_name("generate username"))
            .register_transaction(transaction!(post_message).set_name("post request")),
    )
    //.register_scenario(
    //    scenario!("DeleteOneMailbox")
    //        .register_transaction(transaction!(set_username).set_name("generate username"))
    //        .register_transaction(transaction!(delete_mailbox).set_name("delete mailbox")),
    //)
    //.register_scenario(
    //    scenario!("GetOneMailbox")
    //        .register_transaction(transaction!(set_username).set_name("generate username"))
    //        .register_transaction(transaction!(get_mailbox).set_name("get mailbox")),
    //)
    //.register_scenario(
    //    scenario!("DeletePostGetDelete")
    //        .register_transaction(transaction!(set_username).set_name("generate username"))
    //        .register_transaction(transaction!(delete_mailbox).set_name("delete mailbox"))
    //        .register_transaction(transaction!(post_message).set_name("post request"))
    //        .register_transaction(transaction!(get_mailbox).set_name("get mailbox"))
    //        .register_transaction(transaction!(delete_mailbox).set_name("delete mailbox")),
    //)
    //.register_scenario(
    //    scenario!("ValidateGetMessageAfterPost")
    //        .register_transaction(transaction!(set_username).set_name("generate username"))
    //        .register_transaction(transaction!(loop_message).set_name("loop message")),
    //)
    .execute()
    .await?;

    Ok(())
}

#[derive(Serialize)]
struct Username(String);

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Payload {
    pub c_type: usize,
    pub ciphertext: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub device_id: String,
    pub payload: Payload,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Bundle {
    pub batch: Vec<Message>,
}

// fn generate_username() -> String {
//     let Some(letter) = LETTERS.choose(&mut rand::thread_rng()) else {todo!()};
//     let Some(number) = NUMBERS.choose(&mut rand::thread_rng()) else {todo!()};
//
//     letter.to_string() + &number.to_string()
// }

fn generate_username() -> String {
    (unsafe { USERNAMES.as_ref().unwrap() })
        .choose(&mut rand::thread_rng())
        .unwrap()
        .clone()
}

async fn set_username(user: &mut GooseUser) -> TransactionResult {
    user.set_session_data(Username(generate_username()));

    Ok(())
}

//can optimize if the same for everyone
fn construct_message(sender: String, friends: Vec<String>) -> Bundle {
    let payload = Payload {
        c_type: 0,
        ciphertext: "hello world".to_string(),
    };

    let mut batch = Vec::new();

    batch.push(Message {
        device_id: sender,
        payload: payload.clone(),
    });
    for f in friends {
        batch.push(Message {
            device_id: f,
            payload: payload.clone(),
        })
    }

    return Bundle { batch };
}

fn generate_friends(num_friends: usize) -> Vec<String> {
    let mut friends = Vec::new();

    for i in 0..num_friends {
        friends.push(
            unsafe { FRIENDS.as_ref().unwrap() }
                .choose(&mut rand::thread_rng())
                .unwrap()
                .clone(),
        )
    }
    return friends;
}

async fn post_message(user: &mut GooseUser) -> TransactionResult {
    let sender = &user.get_session_data::<Username>().unwrap().0;

    let mut headers = HeaderMap::new();
    let auth_name = "Bearer ".to_owned() + &sender;
    headers.insert("Authorization", HeaderValue::from_str(&auth_name).unwrap());
    headers.insert(
        "Content-Type",
        HeaderValue::from_str("application/json").unwrap(),
    );

    let request_builder = user
        .get_request_builder(&GooseMethod::Post, "/message")?
        .headers(headers);

    let friends = generate_friends(FRIENDS.unwrap().len() - 1);
    let msg = json!(construct_message(sender.to_owned(), friends));

    let goose_request = GooseRequest::builder()
        .method(GooseMethod::Post)
        .path("/message")
        .set_request_builder(request_builder.json(&msg))
        .build();

    let _goose_metrics = user.request(goose_request).await;

    Ok(())
}

async fn get_mailbox(user: &mut GooseUser) -> TransactionResult {
    let username = &user.get_session_data::<Username>().unwrap().0;

    let mut headers = HeaderMap::new();
    let auth_name = "Bearer ".to_owned() + username.as_str();
    headers.insert("Authorization", HeaderValue::from_str(&auth_name).unwrap());

    let request_builder = user
        .get_request_builder(&GooseMethod::Get, "/outbox")?
        .headers(headers);

    let goose_request = GooseRequest::builder()
        .method(GooseMethod::Get)
        .path("/outbox")
        .set_request_builder(request_builder)
        .build();

    let _goose_metrics = user.request(goose_request).await;

    Ok(())
}

async fn delete_mailbox(user: &mut GooseUser) -> TransactionResult {
    let username = &user.get_session_data::<Username>().unwrap().0;

    let mut headers = HeaderMap::new();
    let auth_name = "Bearer ".to_owned() + username.as_str();
    headers.insert("Authorization", HeaderValue::from_str(&auth_name).unwrap());

    let request_builder = user
        .get_request_builder(&GooseMethod::Delete, "/outbox")?
        .headers(headers);

    let goose_request = GooseRequest::builder()
        .method(GooseMethod::Delete)
        .path("/outbox")
        .set_request_builder(request_builder)
        .build();

    let _goose_metrics = user.request(goose_request).await;

    Ok(())
}

async fn loop_message(user: &mut GooseUser) -> TransactionResult {
    // SEND MESSAGE TO SELF
    let sender = &user.get_session_data::<Username>().unwrap().0;

    let mut headers = HeaderMap::new();
    let auth_name = "Bearer ".to_owned() + &sender;
    headers.insert("Authorization", HeaderValue::from_str(&auth_name).unwrap());
    headers.insert(
        "Content-Type",
        HeaderValue::from_str("application/json").unwrap(),
    );

    let post_request_builder = user
        .get_request_builder(&GooseMethod::Post, "/message")?
        .headers(headers.clone());

    let data = json!(
    {
        "batch":
        [
        {"deviceId": sender,
        "payload": {"cType":0, "ciphertext": "Text to Validate"}},
        ]
    });

    let post_request = GooseRequest::builder()
        .method(GooseMethod::Post)
        .path("/message")
        .set_request_builder(post_request_builder.json(&data))
        .build();

    let _goose_metrics = user.request(post_request).await;

    //// GET MESSAGE BACK
    let get_request_builder = user
        .get_request_builder(&GooseMethod::Get, "/outbox")?
        .headers(headers);

    let get_request = GooseRequest::builder()
        .method(GooseMethod::Get)
        .path("/outbox")
        .set_request_builder(get_request_builder)
        .build();

    let resp = user.request(get_request).await?;

    let validate = &Validate::builder()
        .status(200)
        .text("Text to Validate")
        .build();

    validate_and_load_static_assets(user, resp, &validate).await?;

    Ok(())
}
