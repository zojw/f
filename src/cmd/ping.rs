use bytes::Bytes;

use crate::{
    frame::Frame,
    parse::{Parse, ParseError},
};

/// Returns PONG if no argument is provided, otherwise
/// return a copy of the argument as a bulk.
///
/// This command is often used to test if a connection
/// is still alive, or to measure latency.
#[derive(Debug, Default)]
pub struct Ping {
    /// optional message to be returned
    msg: Option<String>,
}

impl Ping {
    /// Create a new `Ping` command with optional `msg`.
    pub fn new(msg: Option<String>) -> Ping {
        Ping { msg }
    }

    /// Parse a `Ping` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `PING` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `Ping` value on success. If the frame is malformed, `Err` is
    /// returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing `PING` and an optional message.
    ///
    /// ```text
    /// PING [message]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Ping> {
        match parse.next_string() {
            Ok(msg) => Ok(Ping::new(Some(msg))),
            Err(ParseError::EndOfStream) => Ok(Ping::default()),
            Err(e) => Err(crate::Error::Unknown(e.into())),
        }
    }

    /// Apply the `Ping` command and return the message.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    pub(crate) fn apply(self) -> crate::Result<Frame> {
        let response = match self.msg {
            None => Frame::Simple("PONG".to_string()),
            Some(msg) => Frame::Bulk(Bytes::from(msg)),
        };
        Ok(response)
    }
}
