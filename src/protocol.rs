use tokio_util::codec::{Decoder, Encoder};

use crate::data::Bytable;

#[derive(Debug, PartialEq)]
pub struct RequestVoteDesc {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, PartialEq)]
pub struct RequestVoteResponseDesc {
    pub current_term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, PartialEq)]
pub struct AppendEntryDescHeader {
    pub term: u64,
    pub leader_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
    pub commit_index: u64,
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
    pub accepted: bool,
    pub last_log_term: u64,
    pub last_term_first_index: u64,
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
