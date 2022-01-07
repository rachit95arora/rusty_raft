use std::mem::size_of;

use bytes::{Buf, BufMut, BytesMut};

//pub unsafe fn as_u8_slice<T: Sized>(p: &T) -> &[u8] {
//    ::std::slice::from_raw_parts((p as *const T) as *const u8, ::std::mem::size_of::<T>())
//}

pub const CONNECTION_BUF_SIZE: usize = 4 * 1024;
pub const NUM_BUFFERED_LOG_ENTRIES: usize = 10;
type Error = Box<dyn std::error::Error>;

pub trait Bytable {
    fn to_bytes(&self, bytes: &mut BytesMut);
    fn from_bytes(bytes: &mut BytesMut) -> Option<Self>
    where
        Self: Sized;
}

#[derive(Debug, PartialEq)]
pub struct KeyValCommand {
    pub key: u32,
    pub value: u64,
}

impl Bytable for KeyValCommand {
    fn to_bytes(&self, bytes: &mut BytesMut) {
        bytes.put_u32(self.key);
        bytes.put_u64(self.value);
    }

    fn from_bytes(bytes: &mut BytesMut) -> Option<Self>
    where
        Self: Sized,
    {
        if bytes.len() >= size_of::<u32>() + size_of::<u64>() {
            let key = bytes.get_u32();
            let value = bytes.get_u64();
            return Some(KeyValCommand { key, value });
        }
        None
    }
}

#[derive(Debug, PartialEq)]
pub struct Entry<CommandType>
where
    CommandType: Bytable,
{
    pub term: u64,
    pub index: u64,
    pub command: CommandType,
}

impl<CommandType: Bytable> Bytable for Entry<CommandType> {
    fn to_bytes(&self, bytes: &mut BytesMut) {
        bytes.put_u64(self.term);
        bytes.put_u64(self.index);
        self.command.to_bytes(bytes);
    }

    fn from_bytes(bytes: &mut BytesMut) -> Option<Self>
    where
        Self: Sized,
    {
        let term = bytes.get_u64();
        let index = bytes.get_u64();
        if let Some(command) = CommandType::from_bytes(bytes) {
            Some(Self {
                term,
                index,
                command,
            })
        } else {
            None
        }
    }
}
