use std::{
    fmt::{Debug, Display},
    mem::size_of,
};

use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::data::Bytable;

#[derive(Debug)]
pub enum ProtocolError {
    UnknownMessageTag,
}

impl From<std::io::Error> for ProtocolError {
    fn from(_: std::io::Error) -> Self {
        ProtocolError::UnknownMessageTag
    }
}

impl Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownMessageTag => {
                write!(f, "Message with unknown tag encountered while parsing!")
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct RequestVoteDesc {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

impl Bytable for RequestVoteDesc {
    fn to_bytes(&self, bytes: &mut bytes::BytesMut) {
        bytes.reserve(size_of::<u64>() * 4);
        bytes.put_u64(self.term);
        bytes.put_u64(self.candidate_id);
        bytes.put_u64(self.last_log_index);
        bytes.put_u64(self.last_log_term);
    }
    fn length_if_can_parse(bytes: &[u8]) -> Option<usize> {
        let req_len = 4 * size_of::<u64>();
        if bytes.len() >= req_len {
            Some(req_len)
        } else {
            None
        }
    }
    fn from_bytes(bytes: &mut bytes::BytesMut) -> Option<Self>
    where
        Self: Sized,
    {
        if let None = Self::length_if_can_parse(&bytes[..]) {
            return None;
        }
        let term = bytes.get_u64();
        let candidate_id = bytes.get_u64();
        let last_log_index = bytes.get_u64();
        let last_log_term = bytes.get_u64();

        Some(Self {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct RequestVoteResponseDesc {
    pub current_term: u64,
    pub vote_granted: bool,
}

impl Bytable for RequestVoteResponseDesc {
    fn to_bytes(&self, bytes: &mut bytes::BytesMut) {
        bytes.reserve(size_of::<u64>() + size_of::<u8>());
        bytes.put_u64(self.current_term);
        if self.vote_granted {
            bytes.put_u8(1u8);
        } else {
            bytes.put_u8(0u8);
        }
    }
    fn length_if_can_parse(bytes: &[u8]) -> Option<usize> {
        let req_len = size_of::<u64>() + size_of::<u8>();
        if bytes.len() >= req_len {
            Some(req_len)
        } else {
            None
        }
    }
    fn from_bytes(bytes: &mut bytes::BytesMut) -> Option<Self>
    where
        Self: Sized,
    {
        if let None = Self::length_if_can_parse(&bytes[..]) {
            return None;
        }
        let current_term = bytes.get_u64();
        let vote_int = bytes.get_u8();
        let mut vote_granted = false;
        if vote_int > 0 {
            vote_granted = true;
        }
        Some(Self {
            current_term,
            vote_granted,
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct AppendEntryDescHeader {
    pub term: u64,
    pub leader_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
    pub commit_index: u64,
}

impl Bytable for AppendEntryDescHeader {
    fn to_bytes(&self, bytes: &mut bytes::BytesMut) {
        bytes.reserve(size_of::<u64>() * 5);
        bytes.put_u64(self.term);
        bytes.put_u64(self.leader_id);
        bytes.put_u64(self.last_log_index);
        bytes.put_u64(self.last_log_term);
        bytes.put_u64(self.commit_index);
    }
    fn length_if_can_parse(bytes: &[u8]) -> Option<usize> {
        let req_len = 5 * size_of::<u64>();
        if bytes.len() >= req_len {
            Some(req_len)
        } else {
            None
        }
    }
    fn from_bytes(bytes: &mut bytes::BytesMut) -> Option<Self>
    where
        Self: Sized,
    {
        if let None = Self::length_if_can_parse(&bytes[..]) {
            return None;
        }
        let term = bytes.get_u64();
        let leader_id = bytes.get_u64();
        let last_log_index = bytes.get_u64();
        let last_log_term = bytes.get_u64();
        let commit_index = bytes.get_u64();

        Some(Self {
            term,
            leader_id,
            last_log_index,
            last_log_term,
            commit_index,
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct AppendEntryDesc<EntryType>
where
    EntryType: Bytable + Debug,
{
    pub header: AppendEntryDescHeader,
    pub entries: Vec<EntryType>,
}

#[derive(Debug, PartialEq)]
pub struct AppendEntryResponseDesc {
    pub current_term: u64,
    pub last_log_term: u64,
    pub last_term_first_index: u64,
    pub accepted: bool,
}

impl Bytable for AppendEntryResponseDesc {
    fn to_bytes(&self, bytes: &mut bytes::BytesMut) {
        bytes.reserve(3 * size_of::<u64>() + size_of::<u8>());
        bytes.put_u64(self.current_term);
        bytes.put_u64(self.last_log_term);
        bytes.put_u64(self.last_term_first_index);
        if self.accepted {
            bytes.put_u8(1u8);
        } else {
            bytes.put_u8(0u8);
        }
    }
    fn length_if_can_parse(bytes: &[u8]) -> Option<usize> {
        let req_len = 3 * size_of::<u64>() + size_of::<u8>();
        if bytes.len() >= req_len {
            Some(req_len)
        } else {
            None
        }
    }
    fn from_bytes(bytes: &mut bytes::BytesMut) -> Option<Self>
    where
        Self: Sized,
    {
        if let None = Self::length_if_can_parse(&bytes[..]) {
            return None;
        }
        let current_term = bytes.get_u64();
        let last_log_term = bytes.get_u64();
        let last_term_first_index = bytes.get_u64();
        let accepted_int = bytes.get_u8();
        let mut accepted = false;
        if accepted_int > 0 {
            accepted = true;
        }
        Some(Self {
            current_term,
            last_log_term,
            last_term_first_index,
            accepted,
        })
    }
}

#[derive(Debug, PartialEq)]
pub enum Message<EntryType>
where
    EntryType: Bytable + Debug,
{
    RequestVote(RequestVoteDesc),
    AppendEntry(AppendEntryDesc<EntryType>),
    RequestVoteResponse(RequestVoteResponseDesc),
    AppendEntryResponse(AppendEntryResponseDesc),
}

#[repr(u8)]
#[derive(Copy, Clone)]
enum MessageTag {
    RequestVoteTag = 1,
    AppendEntryTag = 2,
    RequestVoteResponseTag = 3,
    AppendEntryResponseTag = 4,
    UnknownTag = 5,
}

impl From<u8> for MessageTag {
    fn from(data: u8) -> Self {
        match data {
            1 => MessageTag::RequestVoteTag,
            2 => MessageTag::AppendEntryTag,
            3 => MessageTag::RequestVoteResponseTag,
            4 => MessageTag::AppendEntryResponseTag,
            _ => MessageTag::UnknownTag,
        }
    }
}

pub struct MessageCodec<EntryType: Bytable + Debug> {
    partial_append_entry: Option<AppendEntryDesc<EntryType>>,
    num_entries_left: Option<u64>,
}

impl<EntryType: Bytable + Debug> Encoder<Message<EntryType>> for MessageCodec<EntryType> {
    type Error = ProtocolError;
    fn encode(
        &mut self,
        item: Message<EntryType>,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        match item {
            Message::RequestVote(desc) => {
                dst.put_u8(MessageTag::RequestVoteTag as u8);
                desc.to_bytes(dst);
            }
            Message::AppendEntry(desc) => {
                dst.put_u8(MessageTag::AppendEntryTag as u8);
                desc.header.to_bytes(dst);
                dst.put_u64(desc.entries.len() as u64);
                for entry in desc.entries {
                    entry.to_bytes(dst);
                }
            }
            Message::RequestVoteResponse(desc) => {
                dst.put_u8(MessageTag::RequestVoteResponseTag as u8);
                desc.to_bytes(dst);
            }
            Message::AppendEntryResponse(desc) => {
                dst.put_u8(MessageTag::AppendEntryResponseTag as u8);
                desc.to_bytes(dst);
            }
        }
        Ok(())
    }
}

impl<EntryType: Bytable + Debug> MessageCodec<EntryType> {
    pub fn new() -> Self {
        MessageCodec {
            partial_append_entry: None,
            num_entries_left: None,
        }
    }
    fn parse_remaining_entries(
        &mut self,
        src: &mut BytesMut,
    ) -> Result<Option<Message<EntryType>>, ProtocolError> {
        if let None = self.num_entries_left {
            if src.len() < size_of::<u64>() {
                return Ok(None);
            }
            self.num_entries_left = Some(src.get_u64());
        }
        let mut num_entries = self.num_entries_left.unwrap();
        while num_entries > 0 {
            if let Some(_len) = EntryType::length_if_can_parse(&src[..]) {
                let append_entry_opt = std::mem::take(&mut self.partial_append_entry);
                let mut append_entry = append_entry_opt.unwrap();
                append_entry
                    .entries
                    .push(EntryType::from_bytes(src).unwrap());
                num_entries -= 1;

                // Update decoder state
                self.num_entries_left = Some(num_entries);
                self.partial_append_entry = Some(append_entry);
            } else {
                return Ok(None);
            }
        }
        assert_eq!(
            num_entries, 0,
            "Only reach here if parsing all entries is complete! Failed!"
        );
        let append_entry_opt = std::mem::take(&mut self.partial_append_entry);
        self.num_entries_left = None;
        Ok(Some(Message::AppendEntry(append_entry_opt.unwrap())))
    }
}

impl<EntryType: Bytable + Debug> Decoder for MessageCodec<EntryType> {
    type Item = Message<EntryType>;
    type Error = ProtocolError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 1 {
            return Ok(None);
        }
        if let None = self.partial_append_entry {
            let message_tag: MessageTag = src.get_u8().into();
            match message_tag {
                MessageTag::AppendEntryTag => {
                    if let Some(_len) = AppendEntryDescHeader::length_if_can_parse(&src[..]) {
                        let header = AppendEntryDescHeader::from_bytes(src).unwrap();
                        self.partial_append_entry = Some(AppendEntryDesc {
                            header,
                            entries: Vec::new(),
                        });
                        self.num_entries_left = None;
                        return self.parse_remaining_entries(src);
                    }
                }
                MessageTag::RequestVoteTag => {
                    if let Some(_len) = RequestVoteDesc::length_if_can_parse(&src[..]) {
                        match RequestVoteDesc::from_bytes(src) {
                            Some(desc) => {
                                return Ok(Some(Message::RequestVote(desc)));
                            }
                            None => {
                                panic!("Parsing implementation bug! RequestVote parsing failed despite checking");
                            }
                        }
                    }
                }
                MessageTag::AppendEntryResponseTag => {
                    if let Some(_len) = AppendEntryResponseDesc::length_if_can_parse(&src[..]) {
                        match AppendEntryResponseDesc::from_bytes(src) {
                            Some(desc) => {
                                return Ok(Some(Message::AppendEntryResponse(desc)));
                            }
                            None => {
                                panic!("Parsing implementation bug! RequestVote parsing failed despite checking");
                            }
                        }
                    }
                }
                MessageTag::RequestVoteResponseTag => {
                    if let Some(_len) = RequestVoteResponseDesc::length_if_can_parse(&src[..]) {
                        match RequestVoteResponseDesc::from_bytes(src) {
                            Some(desc) => {
                                return Ok(Some(Message::RequestVoteResponse(desc)));
                            }
                            None => {
                                panic!("Parsing implementation bug! RequestVote parsing failed despite checking");
                            }
                        }
                    }
                }
                MessageTag::UnknownTag => {
                    return Err(ProtocolError::UnknownMessageTag);
                }
            }
        } else {
            // Parse the remaining entries for the partial append entry
            return self.parse_remaining_entries(src);
        }
        Ok(None)
    }
}
