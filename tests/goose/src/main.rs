use goose::prelude::*;
use rand::seq::SliceRandom;

use reqwest::header::{HeaderMap, HeaderValue};

use serde_json::json;

static LETTERS: [&str; 26] = [
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s",
    "t", "u", "v", "w", "x", "y", "z",
];
static NUMBERS: [&str; 10] = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"];

#[tokio::main]
async fn main() -> Result<(), GooseError> {
    let g = GooseAttack::initialize()?;

    g.register_scenario(scenario!("PostTest").register_transaction(transaction!(post_message)))
        .execute()
        .await?;

    Ok(())
}

fn generate_pair() -> (String, String) {
    let Some(sender_letter) = LETTERS.choose(&mut rand::thread_rng()) else {todo!()};
    let Some(sender_number) = NUMBERS.choose(&mut rand::thread_rng()) else {todo!()};

    // TODO: check not the same
    let Some(rec_letter) = LETTERS.choose(&mut rand::thread_rng()) else {todo!()};
    let Some(rec_number) = NUMBERS.choose(&mut rand::thread_rng()) else {todo!()};

    (
        sender_letter.to_string() + &sender_number.to_string(),
        rec_letter.to_string() + &rec_number.to_string(),
    )
}

async fn post_message(user: &mut GooseUser) -> TransactionResult {
    let (sender, recipient) = generate_pair();

    // Build a custom HeaderMap to include with all requests made by this client.
    let mut headers = HeaderMap::new();

    let auth_name = "Bearer ".to_owned() + sender.as_str();
    headers.insert("Authorization", HeaderValue::from_str(&auth_name).unwrap());
    headers.insert(
        "Content-Type",
        HeaderValue::from_str("application/json").unwrap(),
    );

    let request_builder = user
        .get_request_builder(&GooseMethod::Post, "/message")?
        .headers(headers);

    let data = json!(
    {
        "batch":
        [
        {"deviceId": sender,
        "payload": {"cType":0, "ciphertext": "hello"}},
        { "deviceId": recipient,
        "payload": {"cType":0, "ciphertext": "goodbye"}},
        ]
    });

    let goose_request = GooseRequest::builder()
        .method(GooseMethod::Post)
        .path("/message")
        .set_request_builder(request_builder.json(&data))
        .build();

    let _goose_metrics = user.request(goose_request).await;

    Ok(())
}
