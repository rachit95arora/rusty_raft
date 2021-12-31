use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Command {
    pub key: u32,
    pub value: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Entry<CommandType> {
    pub term: usize,
    pub command: CommandType,
}

pub type CommandType = Command;
pub type EntryType = Entry<CommandType>;
