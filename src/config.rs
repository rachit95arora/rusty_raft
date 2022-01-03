use std::collections::HashMap;

pub struct TopologyConf {
    pub num_peers: u64,
    pub self_id: u64,
    pub id_to_addrs: HashMap<u64, String>,
    pub addrs_to_id: HashMap<String, u64>,
}
