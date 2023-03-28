use goose::prelude::*;
use serde_json::json;
use serde::{Serialize, Deserialize};
use reqwest::header::{HeaderMap,HeaderValue};
use random_string::generate;
use rand::seq::SliceRandom;

static Users : u8 = 1000;
static UserIds : Vec<String> = vec![None,  Users];

fn generate_pair() -> (char, char) {

    let senderId = UserIds.choose(&mut rand::thread_rng());
    let recId = '0';
    while senderId != recId {
        recId = UserIds.choose(&mut rand::thread_rng());
    }

    (senderId, recId)
}

#[tokio::main]
async fn main() -> Result<(), GooseError> {

    let charset = "1234567890abcdefghijklmnopqrstuvwxyz";
    for i in 0..Users {
        UserIds[i as usize ] = generate(2, charset);
    }

    let g = GooseAttack::initialize()?;

    g.register_scenario(scenario!("PostTest").register_transaction(transaction!(post_message))).execute().await?;

    Ok(())
}

async fn post_message(user: &mut GooseUser) -> TransactionResult {

    let (sender, recipient) = generate_pair();

    // Build a custom HeaderMap to include with all requests made by this client.
    let mut headers = HeaderMap::new();
    headers.insert(
        "Authorization",
        HeaderValue::from_str("Bearer ".concat(sender)).unwrap(),
    );
    headers.insert("Content-Type", HeaderValue::from_str("application/json").unwrap());

    let request_builder = user.get_request_builder(&GooseMethod::Post, "/message")?
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
