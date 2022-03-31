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

use crate::cmd::Command;
use crate::conn::Conn;
use crate::db::Db;
use crate::*;
use glommio::net::TcpListener;
use glommio::{CpuSet, LocalExecutorBuilder, LocalExecutorPoolBuilder, Placement, PoolPlacement};
use std::net::SocketAddr;
use std::rc::Rc;

pub fn run(addr: impl Into<SocketAddr>) -> Result<()> {
    let addr = addr.into();
    LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(
        num_cpus::get(),
        CpuSet::online().ok(),
    ))
    .on_all_shards(move || async move {
        let exec_id = glommio::executor().id();
        println!("Starting executor {}", exec_id);
        ShardExecutor {
            addr: addr.clone(),
            db: Rc::new(Db::new()),
        }
        .listen_and_accept()
        .await
        .unwrap();
    })
    .unwrap()
    .join_all();

    // let builder = LocalExecutorBuilder::new(Placement::Fixed(1));
    // let server_handle = builder.name("server").spawn(move || async move {
    //     ShardExecutor {
    //         addr: addr.clone(),
    //         db: Rc::new(Db::new()),
    //     }
    //     .listen_and_accept()
    //     .await
    //     .unwrap();
    // })?;
    // server_handle.join().unwrap();

    Ok(())
}

pub struct ShardExecutor {
    addr: SocketAddr,
    db: Rc<db::Db>,
}

impl ShardExecutor {
    async fn listen_and_accept(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.addr)?;
        loop {
            let socket = listener.accept().await?;
            let addr = socket.peer_addr()?;
            let db = self.db.clone();
            println!("Accept conn from: {}", addr);
            let conn = conn::Conn::new(socket.buffered(), addr);
            glommio::spawn_local(async move {
                Self::handle_conn_cmd(db, conn).await.unwrap();
            })
            .detach();
        }
        Ok(())
    }

    async fn handle_conn_cmd(db: Rc<Db>, mut conn: Conn) -> Result<()> {
        loop {
            let maybe_frame = conn.read_frame().await?;
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };
            let cmd = Command::from_frame(frame)?;
            cmd.apply(db.clone(), &mut conn).await?;
        }
        Ok(())
    }
}
