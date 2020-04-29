// import the apps you want to build
use std::collections::HashMap;
use chronicle_storage::storage::storage::StorageBuilder;
use chronicle_api::api::api::ApiBuilder;
// import launcher macro
use chronicle_common::launcher;

// create event type
launcher!(
    apps_builder: AppsBuilder {storage: StorageBuilder, api: ApiBuilder }, // Apps
    apps: Apps{} // Launcher state
);

// build your apps
impl AppsBuilder {
    fn build(self) -> Apps {
        // - storage app:
        let storage = StorageBuilder::new()
        .listen_address("0.0.0.0:8080".to_string())
        .thread_count(8)
        .local_dc("datacenter1".to_string())
        .reporter_count(1)
        .buffer_size(1024000)
        .recv_buffer_size(1024000)
        .send_buffer_size(1024000)
        .nodes(vec!["172.17.0.2:9042".to_string()]);
        // - api app
        let api = ApiBuilder::new()
        .listen_address("0.0.0.0:4000".to_string());
        // add app to AppsBuilder then transform it to Apps
        self.storage(storage)
        .api(api)
        .to_apps()
    }
}
// launcher event loop
impl Apps {
    async fn run(mut self) {
        while let Some(event) = self.rx.0.recv().await {
            match event {
                Event::RegisterApp(app_name, shutdown_tx) => {
                    // insert app in map
                    self.apps.insert(app_name, shutdown_tx);
                }
                _ => {

                }
            }
        };
    }
}

#[tokio::main(core_threads = 8)]
async fn main() {
    println!("starting chronicle-example");
    AppsBuilder::new()
    .build() // build apps first, then start them in order you want.
    .storage().await // start storage app
    .api().await // start api app
    .run().await; // run should be defined by poweruser,
    // we are working on basic strategies, to simplify the implementation.
}
