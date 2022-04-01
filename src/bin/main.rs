#![feature(io_error_more)]

use f::server;

#[monoio::main]
async fn main() {
    println!("Running http server on 0.0.0.0:36379");
    server::run(([0, 0, 0, 0], 36379)).await.unwrap();
    println!("Http server stopped");
}
