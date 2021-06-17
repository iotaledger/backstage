use backstage::runtime::BackstageRuntime;

#[tokio::main]
async fn main() {
    env_logger::init();
    #[derive(Clone)]
    pub struct NoBounds;
    let runtime = BackstageRuntime::new("no-children-example", NoBounds).expect("runtime to get created");
    runtime.block_on().await;
}
