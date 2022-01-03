use crate::data::as_u8_slice;
use crate::data::Message;
use crate::data::MessageAction;
use crate::data::CONNECTION_BUF_SIZE;
use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use std::marker::PhantomData;
use tokio::io::AsyncReadExt;
use tokio::io::BufWriter;
use tokio::net::TcpStream;
type Error = Box<dyn std::error::Error>;

pub struct Connection<StreamType, EntryType> {
    stream: BufWriter<StreamType>,
    buffer: BytesMut,
    _dummy: PhantomData<EntryType>,
}

impl<StreamType, EntryType> Connection<StreamType, EntryType> {
    pub fn new(socket: StreamType) -> Connection<StreamType, EntryType> {
        Connection {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(CONNECTION_BUF_SIZE),
            _dummy: PhantomData,
        }
    }

    pub async fn read_message(&mut self) -> Result<Option<Message<EntryType>>, Error> {
        loop {
            if let Some(msg) = self.parse_message()? {
                return Ok(Some(msg));
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("Connection reset by peer".into());
                }
            }
        }
    }

    fn parse_message(&mut self) -> Result<Option<Message<EntryType>>, Error> {
        let mut buf = Cursor::new(&self.buffer[..]);
        match crate::data::bytes_to_message(&buf) {
            Ok(Some((msg, sz))) => {
                self.buffer.advance(sz);
                Ok(Some(msg))
            }
            Ok(None) => Ok(None),
            Err(a) => Err(a),
        }
    }

    pub async fn write_messages(&mut self, messages: &[Message<EntryType>]) -> Result<(), Error> {
        for msg in messages {
            match msg {
                Message::RequestVote(desc) => {
                    self.stream.write_u8(MessageAction::ReqVote).await?;
                    self.stream.write_all(unsafe { as_u8_slice(desc) }).await?;
                }
                Message::AppendEntry(desc) => {
                    self.stream.write_u8(MessageAction::AppEnt).await?;
                    self.stream
                        .write_all(unsafe { as_u8_slice(desc.header) })
                        .await?;
                    self.stream.write_u64(desc.entries.len() as u64).await?;
                    for entry in desc.entries() {
                        self.stream.write_all(unsafe { as_u8_slice(entry) }).await?;
                    }
                }
                Message::RequestVoteResponse(desc) => {
                    self.stream.write_u8(MessageAction::ReqVoteRes).await?;
                    self.stream.write_all(unsafe { as_u8_slice(desc) }).await?;
                }
                Message::AppendEntryResponse(desc) => {
                    self.stream.write_u8(MessageAction::AppEntResp).await?;
                    self.stream.write_all(unsafe { as_u8_slice(desc) }).await?;
                }
            }
        }
        self.stream.flush().await;
        self.stream.get_ref().flush().await?;
        Ok(())
    }
}
