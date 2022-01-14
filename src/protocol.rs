use std::{marker::PhantomData, mem::size_of};

use bytes::{Buf, BufMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::data::{Bytable, Error};

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
    EntryType: Bytable,
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
    EntryType: Bytable,
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

struct MessageCodec<EntryType: Bytable> {
    _dummy: PhantomData<EntryType>,
}

impl<EntryType: Bytable> Encoder<Message<EntryType>> for MessageCodec<EntryType> {
    type Error = Error;
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

impl<EntryType: Bytable> Decoder for MessageCodec<EntryType> {
    type Item = Message<EntryType>;
    type Error = Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 1 {
            return Ok(None);
        }
        let message_tag: MessageTag = src.get_u8().into();
        match message_tag {
            MessageTag::AppendEntryTag => {
                if let Some(len) = AppendEntryDescHeader::length_if_can_parse(&src[..]) {
                    if src.len() < size_of::<u64>() + len {
                        return Ok(None);
                    }
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
                return Err("Illegible message type encountered while decoding!".into());
            }
        }
        Ok(None)
    }
}
