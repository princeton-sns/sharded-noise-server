#[actix_web::test]

mod common;

async fn server_accepts_message() {

    crate::common::new_server(1,1,"8081");
    println!("do e2e test");
}
