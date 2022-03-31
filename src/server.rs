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

use crate::*;
use io_uring::{cqueue::Entry, opcode, types, IoUring};
use rlimit::{getrlimit, Resource};
use std::{collections::HashMap, mem, net::TcpListener, os::unix::prelude::AsRawFd};

pub struct Server {
    pub ring: IoUring,

    fds: Vec<i32>,
    ctx_id_gen: u64,
    listen_fd: Option<types::Fixed>,
    inflight_reqs: HashMap<u64, Req>,
}

#[derive(Clone)]
pub struct Req {
    id: u64,
    op: Op,
    fd: types::Fixed,
}

#[derive(Clone)]
pub enum Op {
    Accept,
    Read,
}

impl Server {
    pub fn new(ring: IoUring) -> Self {
        Self {
            ring,
            ctx_id_gen: 1,
            fds: Vec::new(),
            inflight_reqs: HashMap::new(),
            listen_fd: None,
        }
    }

    pub fn run(&mut self, listener: TcpListener) -> Result<()> {
        let (_, mut file_limit) = getrlimit(Resource::NOFILE)?;
        if file_limit > 32768 {
            file_limit = 32768;
        }
        self.ring
            .submitter()
            .register_files(&vec![-1; file_limit as usize])?;
        self.listen_fd = Some(self.register_fd(listener.as_raw_fd())?);
        self.sumbit_accept_op();
        loop {
            let ready = self.ring.submit_and_wait(1)?;
            if ready == 0 {
                continue;
            }
            let cqes = self.ring.completion().collect::<Vec<_>>();
            for cqe in cqes {
                if cqe.result() < 0 {
                    return Err(Error::Internal(format!("{}", cqe.result())));
                }
                let ctx = self.inflight_reqs.get(&cqe.user_data()).unwrap().to_owned();
                match ctx.op {
                    Op::Accept => self.handle_accept(ctx, cqe)?,
                    Op::Read => self.handle_read(ctx, cqe)?,
                }
            }
        }
        Ok(())
    }

    fn sumbit_accept_op(&mut self) {
        let mut sockaddr: libc::sockaddr = unsafe { mem::zeroed() };
        let mut addrlen: libc::socklen_t = mem::size_of::<libc::sockaddr>() as _;
        let accept_e = opcode::Accept::new(self.listen_fd.unwrap(), &mut sockaddr, &mut addrlen);
        unsafe {
            self.ring
                .submission()
                .push(&accept_e.build().user_data(self.ctx_id_gen))
                .expect("queue is full");
        }
        self.inflight_reqs.insert(
            self.ctx_id_gen,
            Req {
                id: self.ctx_id_gen,
                op: Op::Accept,
                fd: self.listen_fd.unwrap(),
            },
        );
        self.ctx_id_gen += 1;
    }

    fn handle_accept(&mut self, req: Req, cqe: Entry) -> Result<()> {
        assert_eq!(req.id, cqe.user_data());
        let conn_fd = self.register_fd(cqe.result())?;
        println!("accept {}", conn_fd.0);
        self.sumbit_accept_op();
        Ok(())
    }

    fn register_fd(&mut self, fd: i32) -> Result<types::Fixed> {
        self.fds.push(fd);
        let idx = (self.fds.len() - 1) as u32;
        self.ring.submitter().register_files_update(idx, &[fd])?;
        Ok(types::Fixed(idx))
    }

    fn handle_read(&self, req: Req, cqe: Entry) -> Result<()> {
        assert_eq!(req.id, cqe.user_data());

        Ok(())
    }
}
