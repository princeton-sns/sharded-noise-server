use goose::prelude::*;
use serde_json::json;
use serde::{Serialize, Deserialize};
use reqwest::header::{HeaderMap,HeaderValue};

#[tokio::main]
async fn main() -> Result<(), GooseError> {
    let g = GooseAttack::initialize()?;

    g.register_scenario(scenario!("PostTest").register_transaction(transaction!(post_message)))
        .execute()
        .await?;

    Ok(())
}

async fn post_message(user: &mut GooseUser) -> TransactionResult {

    // Build a custom HeaderMap to include with all requests made by this client.
    let mut headers = HeaderMap::new();
    headers.insert(
        "Authorization",
        HeaderValue::from_str("Bearer a").unwrap(),
    );
    headers.insert("Content-Type", HeaderValue::from_str("application/json").unwrap());

    let request_builder = user.get_request_builder(&GooseMethod::Post, "/message")?
        .headers(headers);

    let data = json!(
        {
            "batch": 
            [
            {"deviceId": "y",
            "payload": {"cType":0, "ciphertext": "hello"}}

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
