pub mod common {

    /**
     * Instantiate new server
     */
    async fn new_server(inboxes: u16, outboxes: u16, port: &str) {
        use actix::{Addr};

        let sequencer = sequencer::SequencerActor::new().start();

        // Boot up a set of inbox actors:
        let inbox_actors: Vec<Addr<inbox::InboxActor>> = (0..inboxes)
            .map(|id| inbox::InboxActor::new(id).start())
            .collect();

        // Boot up a set of outbox actors:
        let outbox_actors: Vec<(Addr<outbox::ReceiverActor>, Addr<outbox::OutboxActor>)> = (0
            ..outboxes)
            .map(|id| {
                let out_id = outbox::OutboxActor::new(id, sequencer.clone()).start();
                let rec_id = outbox::ReceiverActor::new(id, out_id.clone()).start();
                (rec_id, out_id)
            })
            .collect();

        let state = web::Data::new(AppState {
            sequencer: sequencer.clone(),
            inbox_actors,
            outbox_actors,
        });

        for inbox_actor in state.inbox_actors.iter() {
            inbox_actor
                .send(inbox::Initialize(state.clone().into_inner()))
                .await
                .unwrap();
        }

        for outbox_actor in state.outbox_actors.iter() {
            outbox_actor
                .0
                .send(outbox::Initialize(state.clone().into_inner()))
                .await
                .unwrap();
        }

        sequencer
            .send(sequencer::Initialize(state.clone().into_inner()))
            .await
            .unwrap();

        HttpServer::new(move || {
            App::new()
                .app_data(state.clone())
                .bind(("127.0.0.1", port))?
                .run()
                .await
        }
    }
}

