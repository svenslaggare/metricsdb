mod helpers;
mod storage;
mod model;
mod tags;
mod metric;
mod engine;
mod server;

#[cfg(test)]
mod integration_tests;

#[tokio::main]
async fn main() {
    server::main().await
}
