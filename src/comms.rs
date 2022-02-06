use crate::config::TopologyConf;
use crate::connection::{CommError, Connection};
use crate::data::Bytable;
use crate::protocol::{Message, MessageCodec};
use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::watch;
use tokio_util::codec::Encoder;

#[derive(Debug)]
pub enum Payload<T> {
    ToAll(T),
    ToId(u64, T),
    Halt,
}

struct TcpCommunicatorBackend<EntryType: Bytable + Debug + Send + 'static> {
    config: TopologyConf,
    to_frontend: Sender<Vec<Message<EntryType>>>,
    from_frontend: Receiver<Payload<Vec<Message<EntryType>>>>,
    id_to_sessions: HashMap<u64, SessionDescriptor>,
    halt_sender: watch::Sender<bool>,
    halt_receiver: watch::Receiver<bool>,
    _dummy: PhantomData<EntryType>,
}

struct SessionDescriptor {
    to_stream: Sender<Arc<Bytes>>,
}

pub struct TcpCommunicator<EntryType: Bytable + Debug + Send + 'static> {
    to_backend: Sender<Payload<Vec<Message<EntryType>>>>,
    from_backend: Receiver<Vec<Message<EntryType>>>,
    _dummy: PhantomData<EntryType>,
}

impl<EntryType: Bytable + Debug + Send + 'static> TcpCommunicator<EntryType> {
    pub async fn new(config: TopologyConf) -> Result<Self, CommError> {
        let (to_backend, from_frontend) = channel(10);
        let (to_frontend, from_backend) = channel(10);
        let (halt_sender, halt_receiver) = watch::channel(false);

        tokio::spawn(async move {
            let mut backend = TcpCommunicatorBackend::<EntryType> {
                config,
                to_frontend,
                from_frontend,
                id_to_sessions: HashMap::new(),
                halt_sender,
                halt_receiver,
                _dummy: PhantomData,
            };
            backend.run().await.unwrap();
        });

        Ok(TcpCommunicator {
            to_backend,
            from_backend,
            _dummy: PhantomData,
        })
    }

    pub async fn send_to(
        &mut self,
        id: u64,
        data: Vec<Message<EntryType>>,
    ) -> Result<(), CommError> {
        match self.to_backend.send(Payload::ToId(id, data)).await {
            Ok(_) => return Ok(()),
            Err(_) => return Err(CommError::BackendSendFailure),
        }
    }

    pub async fn send_to_all(&mut self, data: Vec<Message<EntryType>>) -> Result<(), CommError> {
        match self.to_backend.send(Payload::ToAll(data)).await {
            Ok(_) => return Ok(()),
            Err(_) => return Err(CommError::BackendSendFailure),
        }
    }

    pub async fn receive(&mut self) -> Result<Vec<Message<EntryType>>, CommError> {
        match self.from_backend.recv().await {
            Some(val) => Ok(val),
            None => Err(CommError::MissingFrameOnStream),
        }
    }

    pub async fn close(&mut self) -> Result<(), CommError> {
        match self.to_backend.send(Payload::Halt).await {
            Ok(_) => return Ok(()),
            Err(_) => return Err(CommError::BackendSendFailure),
        }
    }
}

impl<EntryType: Bytable + Debug + 'static + Send> TcpCommunicatorBackend<EntryType> {
    async fn run(&mut self) -> Result<(), CommError> {
        let incoming_listener = TcpListener::bind(
            self.config
                .id_to_addrs
                .get(&self.config.self_id)
                .expect("Missing self address in topology config!"),
        )
        .await
        .expect("Failed to bind to self address!");

        let mut codec = MessageCodec::new();
        let mut bytes_mut = BytesMut::new();

        loop {
            tokio::select! {
                send_data = self.from_frontend.recv() => {
                     match send_data.unwrap() {
                         Payload::Halt => {
                             self.halt_sender.send(true).unwrap();
                             break;
                         },
                         Payload::ToId(id, data) => {
                             for msg in data {
                                 match codec.encode(msg, &mut bytes_mut) {
                                     Ok(_) => (),
                                     Err(_) => return Err(CommError::DataEncodingFailure),
                                 }
                             }
                             let bytes = bytes_mut.split_to(bytes_mut.len()).freeze();
                             let bytes = Arc::new(bytes);
                             self.send_to_id(id, &bytes).await?;
                         },
                         Payload::ToAll(data) => {
                             for msg in data {
                                 match codec.encode(msg, &mut bytes_mut) {
                                     Ok(_) => (),
                                     Err(_) => return Err(CommError::DataEncodingFailure),
                                 }
                             }
                             let bytes = bytes_mut.split_to(bytes_mut.len()).freeze();
                             let bytes = Arc::new(bytes);
                             let all_ids : Vec<u64> = self.config.id_to_addrs.keys().cloned().collect();
                             for id in all_ids {
                                 if id != self.config.self_id {
                                    self.send_to_id(id, &bytes).await?;
                                 }
                             }
                         },
                     }
                },
                peer_fut = incoming_listener.accept() => {
                    let (stream, addr) = peer_fut.unwrap();
                    self.initiate_session(stream, &addr.ip().to_string()).await?;
                },
            }
        }
        Ok(())
    }

    async fn send_to_id(&mut self, id: u64, bytes: &Arc<Bytes>) -> Result<(), CommError> {
        if let None = self.id_to_sessions.get(&id) {
            // Initiate a session with target id
            let ip_addr_string = self
                .config
                .id_to_addrs
                .get(&id)
                .expect("No address for id")
                .clone();
            let stream_to_id = match TcpStream::connect(&ip_addr_string).await {
                Ok(stream) => stream,
                Err(_) => return Err(CommError::AddressConnectFailure(ip_addr_string.clone())),
            };
            self.initiate_session(stream_to_id, &ip_addr_string).await?;
        }
        match self
            .id_to_sessions
            .get(&id)
            .unwrap()
            .to_stream
            .send(bytes.clone())
            .await
        {
            Ok(_) => return Ok(()),
            Err(_) => return Err(CommError::StreamSendFailure),
        }
    }

    async fn initiate_session(
        &mut self,
        stream: TcpStream,
        addr_string: &str,
    ) -> Result<(), CommError> {
        let id = *self
            .config
            .addrs_to_id
            .get(addr_string)
            .expect("Missing id for ip address!");

        let mut halt_receiver = self.halt_receiver.clone();

        if let None = self.id_to_sessions.get(&id) {
            let (to_stream, mut from_app) = channel(100);
            let to_app = self.to_frontend.clone();
            tokio::spawn(async move {
                let mut conn = Connection::<EntryType>::new(stream);
                loop {
                    tokio::select! {
                        _halt = halt_receiver.changed() => {
                            if *halt_receiver.borrow() {
                                break;
                            }
                        },
                        payload = from_app.recv() => {
                            conn.write_message(payload.unwrap()).await.unwrap();
                        },
                        res = conn.read_message() => {
                            let data = res.unwrap();
                            to_app.send(vec![data]).await.unwrap();
                        },
                    }
                }
            });
            self.id_to_sessions
                .insert(id, SessionDescriptor { to_stream });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tcp_communicator_tests {
    use std::collections::HashMap;

    use crate::config::TopologyConf;

    fn get_two_peers_config(is_zero: bool) -> TopologyConf {
        let mut self_id = 0u64;
        if !is_zero {
            self_id = 1u64;
        }
        TopologyConf {
            num_peers: 2,
            self_id,
            id_to_addrs: HashMap::from([
                (0u64, String::from("127.0.0.1:54321")),
                (1u64, String::from("127.0.0.1:54322")),
            ]),
            addrs_to_id: HashMap::from([
                (String::from("127.0.0.1:54321"), 0u64),
                (String::from("127.0.0.1:54322"), 1u64),
            ]),
        }
    }
}
