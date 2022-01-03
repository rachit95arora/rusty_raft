use serde::{Deserialize, Serialize};
// use std::io::{Cursor, Read};
// use tokio::io::AsyncReadExt;

pub unsafe fn as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    ::std::slice::from_raw_parts((p as *const T) as *const u8, ::std::mem::size_of::<T>())
}

pub const CONNECTION_BUF_SIZE: usize = 4 * 1024;
type Error = Box<dyn std::error::Error>;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct KeyValCommand {
    pub key: u32,
    pub value: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Entry<CommandType> {
    pub term: u64,
    pub index: u64,
    pub command: CommandType,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct RequestVoteDesc {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct RequestVoteResponseDesc {
    pub current_term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct AppendEntryDescHeader {
    pub term: u64,
    pub leader_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
    pub commit_index: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct AppendEntryDesc<EntryType> {
    pub header: AppendEntryDescHeader,
    pub entries: Vec<EntryType>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct AppendEntryResponseDesc {
    pub current_term: u64,
    pub accepted: bool,
    pub last_log_term: u64,
    pub last_term_first_index: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Message<EntryType> {
    RequestVote(RequestVoteDesc),
    AppendEntry(AppendEntryDesc<EntryType>),
    RequestVoteResponse(RequestVoteResponseDesc),
    AppendEntryResponse(AppendEntryResponseDesc),
}

pub type KeyValMessage = Message<Entry<KeyValCommand>>;

// #[repr(u8)]
// #[derive(Copy)]
// pub enum MessageAction {
//     ReqVote = 1,
//     AppEnt = 2,
//     ReqVoteRes = 3,
//     AppEntResp = 4,
// }
//
// fn bytes_to_simple_struct<VariantT>(buf: &mut Cursor<&[u8]>) -> Option<(VariantT, usize)> {
//     let variant_len: usize = std::mem::size_of::<VariantT>();
//     if buf.get_ref().len() - buf.position() as usize >= variant_len {
//         let message_desc = unsafe {
//             let mut message_buf = std::mem::ManuallyDrop::new(Vec::new());
//             message_buf.reserve(variant_len);
//             buf.read_exact(message_buf);
//             Some((std::mem::transmute(message_buf.as_ptr()), variant_len))
//         };
//     } else {
//         None
//     }
// }
//
// pub fn bytes_to_message<EntryType>(
//     buf: &mut Cursor<&[u8]>,
// ) -> Result<Option<(Message<EntryType>, usize)>, Error> {
//     match buf.read_u8() {
//         MessageAction::ReqVote => {
//             if let Some((desc, sz)) = bytes_to_simple_struct::<RequestVoteDesc>(buf) {
//                 Ok(Some((Message::RequestVote(desc), sz)))
//             } else {
//                 Ok(None)
//             }
//         }
//         MessageAction::AppEnt => {
//             if let Some((header, sz)) = bytes_to_simple_struct::<AppendEntryDescHeader>(buf) {
//                 let entry_len = std::mem::size_of::<EntryType>();
//                 let num_entries = buf.read_u64();
//                 if buf.get_ref().len() - buf.position() as usize
//                     >= (num_entries as usize * entry_len)
//                 {
//                     let mut entries = Vec::new();
//                     entries.reserve(num_entries);
//                     for entry_idx in [0..num_entries] {
//                         entries.push(bytes_to_simple_struct::<EntryType>().unwrap());
//                     }
//                     Ok(Some(Message::AppendEntry(AppendEntryDesc {
//                         header,
//                         entries,
//                     })))
//                 }
//                 Ok(None)
//             } else {
//                 Ok(None)
//             }
//         }
//         MessageAction::ReqVoteRes => {
//             if let Some((desc, sz)) = bytes_to_simple_struct::<RequestVoteResponseDesc>(buf) {
//                 Ok(Some((Message::RequestVoteResponse(desc), sz)))
//             } else {
//                 Ok(None)
//             }
//         }
//         MessageAction::AppEntResp => {
//             if let Some((desc, sz)) = bytes_to_simple_struct::<AppendEntryResponseDesc>(buf) {
//                 Ok(Some((Message::AppendEntryResponse(desc), sz)))
//             } else {
//                 Ok(None)
//             }
//         }
//         _err => Err(format!("Received unknown message variant: {}", _err).into()),
//     }
// }
