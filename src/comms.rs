use crate::config::TopologyConf;
use crate::connection::Connection;
use crate::data::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::watch;
type Error = Box<dyn std::error::Error>;

#[derive(Debug)]
pub enum Payload<T> {
    ToAll(T),
    ToId(u64, T),
    Halt,
}

struct TcpCommunicatorBackend<EntryType> {
    config: TopologyConf,
    to_frontend: Sender<Payload<Vec<Message<EntryType>>>>,
    from_frontend: Receiver<Payload<Vec<Message<EntryType>>>>,
    id_to_sessions: HashMap<u64, SessionDescriptor>,
}

struct SessionDescriptor {
    to_stream: Sender<Payload<Arc<Vec<u8>>>>,
}

pub struct TcpCommunicator<EntryType> {
    to_backend: Sender<Payload<Vec<Message<EntryType>>>>,
    from_backend: Receiver<Payload<Vec<Message<EntryType>>>>,
}

impl<EntryType> TcpCommunicator<EntryType> {
    pub async fn new(config: TopologyConf) -> Result<TcpCommunicator, Error> {
        let (to_backend, from_frontend) = channel(100);
        let (to_frontend, from_backend) = channel(100);

        tokio::spawn(async move {
            let mut backend = TcpCommunicatorBackend::<EntryType> {
                config,
                to_frontend,
                from_frontend,
                id_to_sessions: HashMap::new(),
            };
            backend.run().await;
        });
        Ok(TcpCommunicator {
            to_backend,
            from_backend,
        })
    }

    pub async fn send_to(&mut self, id: u64, data: Vec<Message<EntryType>>) -> Result<(), Error> {
        self.to_backend.send(Payload::ToId(id, data)).await?;
        Ok(())
    }

    pub async fn send_to_all(&mut self, data: Vec<Message<EntryType>>) -> Result<(), Error> {
        self.to_backend.send(Payload::ToAll(data)).await?;
        Ok(())
    }

    pub fn backend_receiver(&mut self) -> &Receiver<Payload<Vec<Message<EntryType>>>> {
        &self.from_backend
    }

    pub async fn close(&mut self) -> Result<(), Error> {
        self.to_backend.send(Payload::Halt).await?;
        Ok(())
    }
}

impl<EntryType> TcpCommunicatorBackend<EntryType> {
    async fn run(&mut self) {
        let mut incoming_listener = TcpListener::bind(
            self.config
                .id_to_addrs
                .get(&self.config.self_id)
                .expect("Missing self address in topology config!"),
        )
        .await
        .expect("Failed to bind to self address!");

        let (mut halt_sender, mut halt_receiver) = watch::channel(false);

        loop {
            tokio::select! {
                send_data = self.from_frontend.recv() => {
                     match send_data.unwrap() {
                         Payload::Halt => {
                             halt_sender.send(true).await.unwrap();
                             break;
                         },
                         Payload::ToId(id, bytes) => {

                         },
                         Payload::ToAll(bytes) => {

                         },
                     }
                },
                peer_fut = incoming_listener.accept() => {
                    let (mut stream, addr) = peer_fut.unwrap();
                    if let None = self.id_to_sessions.get(addr.ip().to_string()) {
                        let (mut to_stream, mut from_app) = channel(100);
                        let mut to_app = self.to_frontend.clone();
                        let mut halt_receiver_clone = halt_receiver.clone();
                        tokio::spawn(async move {
                            let mut conn = Connection::<TcpStream, EntryType>::new(stream);
                            loop {
                                tokio::select!{
                                    halt = halt_receiver_clone.recv() => {
                                        if halt {
                                            break;
                                        }
                                    }
                                    payload = from_app.recv() => {
                                        conn.write_messages(&payload[..]).await.unwrap();
                                    }
                                    res = conn.read_message() => {
                                        if let Ok(optional_msg) = res {
                                            if let Some(msg) = optional_msg {
                                                to_app.send(&[msg]);
                                            }
                                        }
                                        else
                                        {
                                            panic!("Failing with error: {}!", res);
                                        }
                                    }
                                }
                            }
                        });
                        self.id_to_sessions.insert(addr.ip().to_string(), SessionDescriptor { to_stream });
                    }
                },
            }
        }
    }
}
