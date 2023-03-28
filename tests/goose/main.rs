#[tokio::main]
async fn main() -> Result<(), GooseError> {
    let _goose_metrics = GooseAttack::initialize()?
        .register_scenario(
            scenario!("Post a random message")
                .set_weight(40)?
                .set_wait_time(Duration::from_secs(0), Duration::from_secs(3))?
                .register_transaction(
                    transaction!(post_page)
                        .set_name("Bearer a")
                        .set_weight(2)?,
                )
        )
        .set_default(GooseDefault::Host, "https://localhost::8081/")?
        .execute()
        .await?;

    Ok(())
}
