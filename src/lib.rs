// Copyright 2022 The Engula Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![feature(read_buf)]

mod cmd;
mod conn;
mod db;
mod error;
mod frame;
mod parse;
mod server;

pub use self::error::{Error, Result};

#[cfg(test)]
mod test {

    use crate::*;
    use io_uring::IoUring;
    use std::net::TcpListener;

    #[test]
    fn test_server() -> Result<()> {
        let ring = IoUring::builder().dontfork().build(32)?;
        let listener = TcpListener::bind(&format!("127.0.0.1:9999"))?;
        server::Server::new(ring).run(listener)?;
        Ok(())
    }
}
