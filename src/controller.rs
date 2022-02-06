use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    mem::size_of,
};

use bytes::{Buf, BufMut};
use rand::{prelude::ThreadRng, thread_rng, Rng};

use crate::{
    comms::TcpCommunicator,
    config::RaftConf,
    data::{Bytable, Defaultable, Entry, EntryT},
    persistance::{DurableLog, LogError},
    protocol::{
        AppendEntryDesc, AppendEntryDescHeader, AppendEntryResponseDesc, Message, RequestVoteDesc,
        RequestVoteResponseDesc,
    },
};

struct SingleLogAdapter<EntryType: Bytable> {
    log: DurableLog<EntryType>,
    value: Option<EntryType>,
}

impl<EntryType: Bytable + Defaultable + Copy> SingleLogAdapter<EntryType> {
    async fn new(filename: &str) -> Result<Self, LogError> {
        let mut log = Self {
            log: DurableLog::new(filename).await?,
            value: None,
        };
        log.update(EntryType::default()).await?;
        Ok(log)
    }

    async fn clear_entry(&mut self) -> Result<(), LogError> {
        self.log.remove_entries_from(0).await?;
        Ok(())
    }

    async fn update(&mut self, entry: EntryType) -> Result<(), LogError> {
        self.clear_entry().await?;
        self.log.append_entries(&vec![entry]).await?;
        self.value = Some(entry);
        Ok(())
    }

    async fn get(&mut self) -> Result<EntryType, LogError> {
        if let None = self.value {
            let entries = self.log.read_entries(0, 10).await?;
            assert_eq!(1, entries.len());
            self.value = Some(entries[0]);
            Ok(entries[0])
        } else {
            Ok(self.value.unwrap())
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct IndexDesc {
    next_index: u64,
    match_index: u64,
}

#[derive(Debug)]
struct LeaderState {
    map: HashMap<u64, IndexDesc>,
}

#[derive(Debug, Copy, Clone)]
struct PersistentState {
    current_term: u64,
    voted_for_id: Option<u64>,
}

impl Defaultable for PersistentState {
    fn default() -> Self {
        PersistentState {
            current_term: 0,
            voted_for_id: None,
        }
    }
}

impl Bytable for PersistentState {
    fn to_bytes(&self, bytes: &mut bytes::BytesMut) {
        bytes.put_u64(self.current_term);
        let voted_for_id = match self.voted_for_id {
            None => u64::MAX,
            Some(val) => val,
        };
        bytes.put_u64(voted_for_id);
    }
    fn from_bytes(bytes: &mut bytes::BytesMut) -> Option<Self>
    where
        Self: Sized,
    {
        if let Some(_len) = Self::length_if_can_parse(&bytes[..]) {
            let current_term = bytes.get_u64();
            let voted_for_id = match bytes.get_u64() {
                u64::MAX => None,
                val => Some(val),
            };
            return Some(Self {
                current_term,
                voted_for_id,
            });
        }
        None
    }
    fn length_if_can_parse(bytes: &[u8]) -> Option<usize> {
        let data_len = size_of::<u64>() * 2;
        if bytes.len() >= data_len {
            return Some(data_len);
        }
        None
    }
}

#[derive(Debug, PartialEq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

struct State {
    role: Role,
    required_votes: usize,
    last_applied_index: u64,
    commit_index: u64,
    persist_state: SingleLogAdapter<PersistentState>,
    timers_since_hb: u64,
    timers_tolerated: u64,
    leader_state: Option<LeaderState>,
}

struct RaftController<CommandType>
where
    CommandType: Bytable + Debug + Send + 'static + Copy + Clone,
{
    comms: TcpCommunicator<Entry<CommandType>>,
    config: RaftConf<CommandType>,
    st: State,
    log: DurableLog<Entry<CommandType>>,
    interval: tokio::time::Interval,
    random_gen: ThreadRng,
}

#[derive(Debug)]
enum ControllerError {
    CreationError,
    AppSendFailure,
    StateUpdateError,
    PeerCommsSendError,
}

impl Display for ControllerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CreationError => {
                write!(f, "Error creating controller instance!")
            }
            Self::AppSendFailure => {
                write!(f, "Error sending log entries to app!")
            }
            Self::StateUpdateError => {
                write!(f, "Error updating persistent state!")
            }
            Self::PeerCommsSendError => {
                write!(f, "Error sending data to raft peers")
            }
        }
    }
}

impl<CommandType: Bytable + Debug + Send + 'static + Copy + Clone> RaftController<CommandType> {
    fn new(conf: RaftConf<CommandType>) -> Result<Self, ControllerError> {
        Err(ControllerError::CreationError)
    }

    async fn run_loop(&mut self) -> Result<(), ControllerError> {
        loop {
            tokio::select! {
                peer_message = self.comms.receive() => {
                    if self.st.role == Role::Leader {

                    }
                    else if self.st.role == Role::Follower {

                    }
                    else {
                        // if role is Candidate
                    }
                },
                client_request = self.config.request_receiver.recv() => {
                    let client_request = client_request.unwrap();
                    if Role::Leader == self.st.role {
                        // Handle client requests
                    }
                    else {
                        // Safely reject
                    }
                },
                _timer_instance = self.interval.tick() => {
                    self.on_timeout().await?;
                },
                _halt = self.config.halt_receiver.changed() => {
                    if *self.config.halt_receiver.borrow() {
                        match self.comms.close().await {
                            Ok(_) => break,
                            Err(_) => return Err(ControllerError::PeerCommsSendError),
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn on_timeout(&mut self) -> Result<(), ControllerError> {
        if Role::Leader == self.st.role {
            self.st.timers_since_hb = 0;
            // Send heartbeats to all peers
            let last_entry = self
                .log
                .read_entries(self.log.total_entries() - 1, 1)
                .await
                .unwrap()[0];
            let header = AppendEntryDescHeader {
                leader_id: self.config.topology_config.self_id,
                term: self.st.persist_state.get().await.unwrap().current_term,
                last_log_index: self.log.total_entries() as u64 - 1,
                last_log_term: last_entry.term(),
                commit_index: self.st.commit_index,
            };
            self.comms
                .send_to_all(vec![Message::AppendEntry(AppendEntryDesc {
                    header,
                    entries: vec![],
                })])
                .await
                .unwrap();
        } else if self.st.timers_since_hb > self.st.timers_tolerated {
            // No hope, time to start elections by self
            self.start_elections().await?;
        } else {
            self.st.timers_since_hb += 1;
        }

        // Apply any new log updates
        self.send_new_commits_to_app().await?;
        Ok(())
    }

    async fn send_new_commits_to_app(&mut self) -> Result<(), ControllerError> {
        if self.st.commit_index > self.st.last_applied_index {
            let new_entries = self
                .log
                .read_entries(
                    self.st.last_applied_index as usize + 1,
                    (self.st.commit_index - self.st.last_applied_index) as usize,
                )
                .await
                .unwrap();
            match self.config.logs_to_app.send(new_entries).await {
                Ok(_) => Ok(()),
                Err(_) => Err(ControllerError::AppSendFailure),
            }
        } else {
            // nothing to send to app
            Ok(())
        }
    }

    async fn start_elections(&mut self) -> Result<(), ControllerError> {
        // Convert to a candidate
        self.st.role = Role::Candidate;
        // Setup required votes before leadership
        self.st.required_votes = self.majority_size() - 1;
        let mut persist_state = self.st.persist_state.get().await.unwrap();
        persist_state.current_term += 1;
        persist_state.voted_for_id = Some(self.config.topology_config.self_id);
        // Update current term and voted for in the persistent state
        self.st.persist_state.update(persist_state).await.unwrap();

        let last_entry = self
            .log
            .read_entries(self.log.total_entries() - 1, 1)
            .await
            .unwrap()[0];
        // Reset timers required for re-election
        self.st.timers_since_hb = 0;
        self.generate_random_hbt_tolerance();
        // Send vote request to peers
        match self
            .comms
            .send_to_all(vec![Message::RequestVote(RequestVoteDesc {
                term: persist_state.current_term,
                candidate_id: self.config.topology_config.self_id,
                last_log_index: self.log.total_entries() as u64 - 1,
                last_log_term: last_entry.term(),
            })])
            .await
        {
            Ok(_) => return Ok(()),
            Err(_) => return Err(ControllerError::PeerCommsSendError),
        }
    }

    async fn process_append_entry(
        &mut self,
        append_entries: &AppendEntryDesc<Entry<CommandType>>,
    ) -> Result<(), ControllerError> {
        let pstate = self.st.persist_state.get().await.unwrap();
        if append_entries.header.term >= pstate.current_term {
            if self.log.total_entries() > append_entries.header.last_log_index as usize {
                let entry = self
                    .log
                    .read_entries(append_entries.header.last_log_index as usize, 1)
                    .await
                    .unwrap()[0];
                if entry.term() == append_entries.header.last_log_term {
                    // Delete redundant entries
                    match self
                        .log
                        .remove_entries_from(append_entries.header.last_log_index as usize + 1)
                        .await
                    {
                        Ok(_) => (),
                        Err(_) => return Err(ControllerError::StateUpdateError),
                    }
                    // Accept new entries
                    match self.log.append_entries(&append_entries.entries).await {
                        Ok(_) => (),
                        Err(_) => return Err(ControllerError::StateUpdateError),
                    }
                    // Reply to leader of acceptance
                    match self
                        .comms
                        .send_to(
                            append_entries.header.leader_id,
                            vec![Message::AppendEntryResponse(AppendEntryResponseDesc {
                                current_term: pstate.current_term,
                                last_log_term: 0,
                                last_term_first_index: 0,
                                accepted: true,
                            })],
                        )
                        .await
                    {
                        Ok(_) => return Ok(()),
                        Err(_) => return Err(ControllerError::PeerCommsSendError),
                    }
                } else {
                    // disagreement with leader at index last_log_index, find first entry with that
                    // term
                    let last_term_first_index = self
                        .find_first_entry_with_term(
                            0,
                            append_entries.header.last_log_index,
                            entry.term(),
                        )
                        .await;
                    match self
                        .comms
                        .send_to(
                            append_entries.header.leader_id,
                            vec![Message::AppendEntryResponse(AppendEntryResponseDesc {
                                current_term: pstate.current_term,
                                last_log_term: entry.term(),
                                last_term_first_index,
                                accepted: false,
                            })],
                        )
                        .await
                    {
                        Ok(_) => return Ok(()),
                        Err(_) => return Err(ControllerError::PeerCommsSendError),
                    }
                }
            } else {
                // We do not have enough entries to append these entries
                let last_entry = self
                    .log
                    .read_entries(self.log.total_entries() - 1, 1)
                    .await
                    .unwrap()[0];
                match self
                    .comms
                    .send_to(
                        append_entries.header.leader_id,
                        vec![Message::AppendEntryResponse(AppendEntryResponseDesc {
                            current_term: pstate.current_term,
                            last_log_term: last_entry.term(),
                            last_term_first_index: self.log.total_entries() as u64 - 1,
                            accepted: false,
                        })],
                    )
                    .await
                {
                    Ok(_) => return Ok(()),
                    Err(_) => return Err(ControllerError::PeerCommsSendError),
                }
            }
        } else {
            // leader term is not up to date, reject and inform them
            match self
                .comms
                .send_to(
                    append_entries.header.leader_id,
                    vec![Message::AppendEntryResponse(AppendEntryResponseDesc {
                        current_term: pstate.current_term,
                        last_log_term: 0,
                        last_term_first_index: 0,
                        accepted: false,
                    })],
                )
                .await
            {
                Ok(_) => return Ok(()),
                Err(_) => return Err(ControllerError::PeerCommsSendError),
            }
        }
    }

    async fn process_request_vote(
        &mut self,
        vote_request: &RequestVoteDesc,
    ) -> Result<(), ControllerError> {
        let mut pstate = self.st.persist_state.get().await.unwrap();
        // at least one entry in log (sentinel term 0 index 0)
        let last_log_entry = self
            .log
            .read_entries(self.log.total_entries() - 1, 1)
            .await
            .unwrap()[0];
        // the candidate term is upto date
        if pstate.current_term <= vote_request.term {
            // node has not voted or voted for this candidate itself
            if pstate.voted_for_id.is_none()
                || pstate.voted_for_id.unwrap() == vote_request.candidate_id
            {
                // Check if candidate log is upto date
                if last_log_entry.term() < vote_request.last_log_term
                    || (last_log_entry.term() == vote_request.last_log_term
                        && self.log.total_entries() <= 1 + vote_request.last_log_index as usize)
                {
                    // Grant vote
                    if let None = pstate.voted_for_id {
                        pstate.voted_for_id = Some(vote_request.candidate_id);
                        match self.st.persist_state.update(pstate).await {
                            Ok(_) => (),
                            Err(_) => return Err(ControllerError::StateUpdateError),
                        }
                    }
                    // Send vote grant
                    match self
                        .comms
                        .send_to(
                            vote_request.candidate_id,
                            vec![Message::RequestVoteResponse(RequestVoteResponseDesc {
                                current_term: pstate.current_term,
                                vote_granted: true,
                            })],
                        )
                        .await
                    {
                        Ok(_) => return Ok(()),
                        Err(_) => return Err(ControllerError::PeerCommsSendError),
                    }
                }
            }
        }
        // Send vote rejection
        match self
            .comms
            .send_to(
                vote_request.candidate_id,
                vec![Message::RequestVoteResponse(RequestVoteResponseDesc {
                    current_term: pstate.current_term,
                    vote_granted: false,
                })],
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(_) => return Err(ControllerError::PeerCommsSendError),
        }
    }

    async fn find_first_entry_with_term(&mut self, mut start: u64, mut end: u64, term: u64) -> u64 {
        while start < end {
            let mid = start + (end - start) / 2;
            let mid_entry = self.log.read_entries(mid as usize, 1).await.unwrap()[0];
            if mid_entry.term() >= term {
                end = mid;
            } else {
                start = mid + 1;
            }
        }
        start
    }

    fn majority_size(&self) -> usize {
        (self.config.topology_config.num_peers / 2 + 1) as usize
    }

    fn is_leader(&self) -> bool {
        self.st.role == Role::Leader
    }

    fn initialise_leader_state(&mut self) {
        let mut lst = LeaderState {
            map: HashMap::new(),
        };
        for peer in self.config.topology_config.id_to_addrs.keys().cloned() {
            if peer != self.config.topology_config.self_id {
                // Not me
                lst.map.insert(
                    peer,
                    IndexDesc {
                        next_index: self.log.total_entries() as u64,
                        match_index: 0,
                    },
                );
            }
        }
        self.st.leader_state = Some(lst);
    }

    fn generate_random_hbt_tolerance(&mut self) {
        self.st.timers_tolerated = self.config.missed_hbt_tolerance_min
            + (self.random_gen.gen::<u64>() % (2 * self.config.topology_config.num_peers));
    }
}
