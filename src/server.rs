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

use crate::{cmd::Command, db::Db, *};
use bytes::{BufMut, BytesMut};
use io_uring::{
    cqueue::Entry,
    opcode,
    types::{self, Fixed},
    IoUring,
};
use rlimit::{getrlimit, Resource};
use std::{
    collections::HashMap,
    io,
    mem::{self, MaybeUninit},
    net::TcpListener,
    os::unix::prelude::AsRawFd,
    rc::Rc,
};

pub struct Server {
    pub ring: IoUring,

    fds: Vec<i32>,
    ctx_id_gen: u64,
    listen_fd: Option<types::Fixed>,
    inflight_reqs: HashMap<u64, Ctx>,
}

#[derive(Clone)]
pub struct Ctx {
    id: u64,
    op: Op,
    fd: types::Fixed,
    rbuf: BytesMut,
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

        let db = Rc::new(Db::new());

        self.ring
            .submitter()
            .register_files(&vec![-1; file_limit as usize])?;
        self.listen_fd = Some(self.register_fd(listener.as_raw_fd())?);
        self.submit_accept_op();
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
                let n = cqe.result();
                let ctx = self.inflight_reqs.get_mut(&cqe.user_data()).unwrap();
                match ctx.op {
                    Op::Accept => {
                        assert_eq!(ctx.id, cqe.user_data());
                        let conn_fd = self.register_fd(cqe.result())?;
                        println!("accept {}", conn_fd.0);
                        self.submit_accept_op();
                        self.submit_read_op(conn_fd);
                    }
                    Op::Read => {
                        assert_eq!(ctx.id, cqe.user_data());
                        if n < 0 {
                            return Err(Error::Io(io::Error::last_os_error()));
                        }
                        let dst = ctx.rbuf.chunk_mut();
                        let dst = unsafe { &mut *(dst as *mut _ as *mut [MaybeUninit<u8>]) };
                        let mut buf = std::io::ReadBuf::uninit(dst);
                        unsafe {
                            buf.assume_init(n as usize);
                            buf.add_filled(n as usize);
                        }
                        let buf = buf.filled();
                        let maybe_frame = conn::parse_frame(buf)?;
                        let frame = match maybe_frame {
                            Some(frame) => frame,
                            None => return Ok(()),
                        };
                        let cmd = Command::from_frame(frame)?;
                        let oframe = cmd.apply(db.clone())?;
                        let conn_fd = ctx.fd.clone();
                        self.submit_write_op(conn_fd, oframe);
                        self.submit_read_op(conn_fd);
                    }
                }
            }
        }
        Ok(())
    }

    fn submit_accept_op(&mut self) {
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
            Ctx {
                id: self.ctx_id_gen,
                op: Op::Accept,
                fd: self.listen_fd.unwrap(),
                rbuf: BytesMut::new(),
            },
        );
        self.ctx_id_gen += 1;
    }

    fn submit_read_op(&mut self, fd: Fixed) {
        let mut ctx = Ctx {
            id: self.ctx_id_gen,
            op: Op::Read,
            fd,
            rbuf: BytesMut::with_capacity(1024),
        };
        self.ctx_id_gen += 1;
        let dst = ctx.rbuf.chunk_mut();
        let dst = unsafe { &mut *(dst as *mut _ as *mut [MaybeUninit<u8>]) };
        let mut buf = std::io::ReadBuf::uninit(dst);
        unsafe {
            let unfilled: &mut [MaybeUninit<u8>] = buf.unfilled_mut();
            let read_e = opcode::Read::new(fd, unfilled.as_mut_ptr().cast(), unfilled.len() as u32);
            self.ring
                .submission()
                .push(&read_e.build().user_data(ctx.id))
                .expect("queue is full");
        }
        self.inflight_reqs.insert(ctx.id, ctx);
    }

    fn register_fd(&mut self, fd: i32) -> Result<types::Fixed> {
        self.fds.push(fd);
        let idx = (self.fds.len() - 1) as u32;
        self.ring.submitter().register_files_update(idx, &[fd])?;
        Ok(types::Fixed(idx))
    }

    pub(crate) fn submit_write_op(&self, conn_fd: Fixed, oframe: frame::Frame) {
        todo!()
    }
}
