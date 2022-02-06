use std::{collections::HashMap, fmt::Debug};

use tokio::sync::{
    mpsc::{Receiver, Sender},
    watch,
};

use crate::data::{Bytable, Entry, Request};

pub struct TopologyConf {
    pub num_peers: u64,
    pub self_id: u64,
    pub id_to_addrs: HashMap<u64, String>,
    pub addrs_to_id: HashMap<String, u64>,
}

pub struct RaftConf<CommandType: Bytable + 'static + Debug + Send + Copy + Clone> {
    pub topology_config: TopologyConf,
    pub granular_timeout_ms: u64,
    pub missed_hbt_tolerance_min: u64,
    pub request_receiver: Receiver<Request<CommandType>>,
    pub logs_to_app: Sender<Vec<Entry<CommandType>>>,
    pub halt_receiver: watch::Receiver<bool>,
}
