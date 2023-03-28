use actix::Addr;
use actix_web::{
    delete, error, get, http::header, post, web, App, HttpMessage, HttpServer, Responder,
};

pub mod sequencer;
pub mod shard;

// const INBOX_ACTORS: u16 = 32;
// const OUTBOX_ACTORS: u16 = 32;

// pub struct AppState {
//     inbox_actors: Vec<Addr<inbox::InboxActor>>,
//     outbox_actors: Vec<(Addr<outbox::ReceiverActor>, Addr<outbox::OutboxActor>)>,
//     sequencer: Addr<sequencer::SequencerActor>,
// }

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // use actix::{Actor, Addr};

    // let sequencer = sequencer::SequencerActor::new().start();

    // // Boot up a set of inbox actors:
    // let inbox_actors: Vec<Addr<shard::inbox::InboxActor>> = (0..INBOX_ACTORS)
    //     .map(|id| inbox::InboxActor::new(id).start())
    //     .collect();

    // // Boot up a set of outbox actors:
    // let outbox_actors: Vec<(Addr<shard::outbox::ReceiverActor>, Addr<shard::outbox::OutboxActor>)> = (0
    //     ..OUTBOX_ACTORS)
    //     .map(|id| {
    //         let out_id = shard::outbox::OutboxActor::new(id, sequencer.clone()).start();
    //         let rec_id = shard::outbox::ReceiverActor::new(id, out_id.clone()).start();
    //         (rec_id, out_id)
    //     })
    //     .collect();

    // let state = web::Data::new(AppState {
    //     _sequencer: sequencer.clone(),
    //     inbox_actors,
    //     outbox_actors,
    // });

    // for inbox_actor in state.inbox_actors.iter() {
    //     inbox_actor
    //         .send(shard::inbox::Initialize(state.clone().into_inner()))
    //         .await
    //         .unwrap();
    // }

    // for outbox_actor in state.outbox_actors.iter() {
    //     outbox_actor
    //         .0
    //         .send(shard::outbox::Initialize(state.clone().into_inner()))
    //         .await
    //         .unwrap();
    // }

    // sequencer
    //     .send(sequencer::Initialize(state.clone().into_inner()))
    //     .await
    //     .unwrap();

    let app_closure = shard::init(
        "http://localhost:8081".to_string(),
        "http://localhost:8082".to_string(),
        8,
        8,
    )
    .await;

    HttpServer::new(move || App::new().configure(app_closure.clone()))
        .bind(("127.0.0.1", 8081))?
        .run()
        .await
}
