use backstage::runtime::BackstageRuntime;

#[tokio::main]
async fn main() {
    env_logger::init();
    let runtime = BackstageRuntime::new("no-children-example").expect("runtime to get created");
    runtime.block_on().await;
}
