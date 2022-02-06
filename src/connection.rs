use crate::data::Bytable;
use crate::data::CONNECTION_BUF_SIZE;
use crate::protocol::Message;
use crate::protocol::MessageCodec;
use crate::protocol::ProtocolError;
use bytes::Bytes;
use futures::StreamExt;
use std::fmt::Debug;
use std::fmt::Display;
use std::sync::Arc;
use tokio::io::split;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::io::ReadHalf;
use tokio::io::WriteHalf;
use tokio::net::TcpStream;
use tokio_util::codec::FramedRead;

#[derive(Debug)]
pub enum CommError {
    MissingFrameOnStream,
    StreamWriteFailure,
    BackendSendFailure,
    DataEncodingFailure,
    AddressConnectFailure(String),
    FailedFrameParse(ProtocolError),
    StreamSendFailure,
}

impl Display for CommError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommError::MissingFrameOnStream => {
                write!(f, "Found no frame on Tcp stream")
            }
            CommError::StreamWriteFailure => {
                write!(f, "Failed to write message to stream")
            }
            CommError::BackendSendFailure => {
                write!(f, "Failed to send payload to comms backend!")
            }
            CommError::FailedFrameParse(err) => {
                write!(f, "Failed to parse next frame with error: {}", err)
            }
            CommError::DataEncodingFailure => {
                write!(f, "Failed to encode user data to bytes!")
            }
            CommError::AddressConnectFailure(addr) => {
                write!(f, "Failed to connect to ip address: {}", addr)
            }
            CommError::StreamSendFailure => {
                write!(f, "Failed to send encoded bytes over TcpStream!")
            }
        }
    }
}
pub struct Connection<EntryType>
where
    EntryType: Bytable + Debug,
{
    read_stream: FramedRead<ReadHalf<TcpStream>, MessageCodec<EntryType>>,
    write_stream: BufWriter<WriteHalf<TcpStream>>,
}

impl<EntryType: Bytable + Debug> Connection<EntryType> {
    pub fn new(socket: TcpStream) -> Self {
        let (read_end, write_end) = split(socket);
        Connection {
            read_stream: FramedRead::with_capacity(
                read_end,
                MessageCodec::new(),
                CONNECTION_BUF_SIZE,
            ),
            write_stream: BufWriter::with_capacity(CONNECTION_BUF_SIZE, write_end),
        }
    }

    pub async fn read_message(&mut self) -> Result<Message<EntryType>, CommError> {
        let result = self.read_stream.next().await;
        match result {
            Some(Ok(message)) => Ok(message),
            Some(Err(error)) => Err(CommError::FailedFrameParse(error)),
            None => Err(CommError::MissingFrameOnStream),
        }
    }

    pub async fn write_message(&mut self, message: Arc<Bytes>) -> Result<(), CommError> {
        match self.write_stream.write_all(&message[..]).await {
            Ok(_) => (),
            Err(_) => return Err(CommError::StreamWriteFailure),
        }
        match self.write_stream.flush().await {
            Ok(_) => (),
            Err(_) => return Err(CommError::StreamWriteFailure),
        }
        Ok(())
    }
}
