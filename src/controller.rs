use std::collections::HashMap;
use tokio::net::{TcpListener, TcpStream};

struct RaftConfig {
    self_id: u32,
    peer_count: u32,
    peer_ids: Vec<u32>,
    addresses: HashMap<u32, String>,
}

struct Controller {
    peer_streams: Vec<TcpStream>,
    address_to_id: HashMap<String, u32>,
    address_to_stream: HashMap<String, u32>,
    raft_config: RaftConfig,
}
