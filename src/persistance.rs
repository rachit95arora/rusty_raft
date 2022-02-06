use bytes::BytesMut;
use std::{fmt::Display, io::SeekFrom, marker::PhantomData, mem::size_of};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter};

use crate::data::{Bytable, NUM_BUFFERED_LOG_ENTRIES};

#[derive(Debug)]
pub enum LogError {
    FileOpenFailed(String),
    SeekError(SeekFrom),
    UnexpectedEof,
    WriteFailure,
    ReadFailure,
    TruncateFailure(u64),
    SyncFailure,
}

impl Display for LogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FileOpenFailed(file) => {
                write!(f, "Failed to open file: {} for durable logger", file)
            }
            Self::SeekError(seek_from) => {
                write!(f, "Failed to seek file to index: {:?}", seek_from)
            }
            Self::UnexpectedEof => {
                write!(f, "Encountered unexpected EOF in logger file")
            }
            Self::WriteFailure => {
                write!(f, "Failed to write to log file")
            }
            Self::ReadFailure => {
                write!(f, "Failed to read from log file")
            }
            Self::TruncateFailure(len) => {
                write!(f, "Failed to truncate log file to len: {}", len)
            }
            Self::SyncFailure => {
                write!(f, "Failed to sync changes to log file")
            }
        }
    }
}
pub struct DurableLog<Entry>
where
    Entry: Bytable,
{
    filename: String,
    filestream: BufWriter<tokio::fs::File>,
    index: Vec<u64>,
    buffer: BytesMut,
    dummy: PhantomData<Entry>,
}

impl<Entry: Bytable> DurableLog<Entry> {
    const ENTRY_SIZE: usize = size_of::<Entry>();
    pub async fn new(filename: &str) -> Result<DurableLog<Entry>, LogError> {
        let file = match tokio::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(filename)
            .await
        {
            Ok(f) => f,
            _ => {
                return Err(LogError::FileOpenFailed(String::from(filename)));
            }
        };

        let buffer = BytesMut::with_capacity(Self::ENTRY_SIZE * 2);

        let mut log = DurableLog {
            filename: String::from(filename),
            filestream: BufWriter::with_capacity(Self::ENTRY_SIZE * NUM_BUFFERED_LOG_ENTRIES, file),
            index: vec![0],
            buffer,
            dummy: PhantomData,
        };

        log.index_all_log().await?;

        Ok(log)
    }

    async fn index_all_log(&mut self) -> Result<(), LogError> {
        let mut running_index = 0u64;
        match self.filestream.seek(SeekFrom::Start(0)).await {
            Ok(_) => (),
            Err(_) => return Err(LogError::SeekError(SeekFrom::Start(0))),
        }
        loop {
            match self.filestream.read_u32().await {
                Ok(frame_len) => {
                    running_index += size_of::<u32>() as u64 + frame_len as u64;
                    match self
                        .filestream
                        .seek(std::io::SeekFrom::Start(running_index))
                        .await
                    {
                        Ok(_) => (),
                        Err(_) => return Err(LogError::SeekError(SeekFrom::Start(running_index))),
                    }
                    self.index.push(running_index);
                }
                Err(err) => {
                    if err.kind() == tokio::io::ErrorKind::UnexpectedEof {
                        return Ok(());
                    }
                    return Err(LogError::UnexpectedEof);
                }
            }
        }
    }

    pub fn total_entries(&self) -> usize {
        self.index.len() - 1
    }

    pub async fn append_entries(&mut self, entries: &Vec<Entry>) -> Result<usize, LogError> {
        match self.filestream.seek(std::io::SeekFrom::End(0)).await {
            Ok(_) => (),
            Err(err) => return Err(LogError::SeekError(SeekFrom::End(0))),
        }
        for entry in entries.iter() {
            self.buffer.clear();
            self.buffer.reserve(2 * Self::ENTRY_SIZE);
            entry.to_bytes(&mut self.buffer);

            match self.filestream.write_u32(self.buffer.len() as u32).await {
                Ok(_) => (),
                Err(_) => return Err(LogError::WriteFailure),
            }
            match self.filestream.write_all(self.buffer.as_ref()).await {
                Ok(_) => (),
                Err(_) => return Err(LogError::WriteFailure),
            }

            self.index.push(
                size_of::<u32>() as u64 + self.buffer.len() as u64 + *self.index.last().unwrap(),
            )
        }
        match self.filestream.flush().await {
            Ok(_) => (),
            Err(_) => return Err(LogError::WriteFailure),
        }
        match self.filestream.get_ref().sync_all().await {
            Ok(_) => (),
            Err(_) => return Err(LogError::WriteFailure),
        }
        Ok(self.total_entries())
    }

    pub async fn read_entries(
        &mut self,
        index: usize,
        count: usize,
    ) -> Result<Vec<Entry>, LogError> {
        let seek_index = std::cmp::min(self.total_entries(), index);
        let seek_offset = *self.index.get(seek_index).unwrap();
        match self.filestream.seek(SeekFrom::Start(seek_offset)).await {
            Ok(_) => (),
            Err(_) => return Err(LogError::SeekError(SeekFrom::Start(seek_offset))),
        }
        let mut read_count = std::cmp::min(count, self.total_entries() - seek_index);
        let mut entries = Vec::with_capacity(read_count);
        while read_count > 0 {
            let frame_len = match self.filestream.read_u32().await {
                Ok(len) => len,
                Err(_) => return Err(LogError::ReadFailure),
            };
            self.buffer.clear();
            self.buffer.reserve(frame_len as usize);
            unsafe {
                self.buffer.set_len(frame_len as usize);
            }
            match self.filestream.read_exact(self.buffer.as_mut()).await {
                Ok(_) => (),
                Err(_) => return Err(LogError::ReadFailure),
            }
            entries.push(Entry::from_bytes(&mut self.buffer).unwrap());
            read_count -= 1;
        }
        Ok(entries)
    }

    pub async fn remove_entries_from(&mut self, index: usize) -> Result<(), LogError> {
        let num_entries = std::cmp::min(self.total_entries(), index);
        let remove_from = *self.index.get(num_entries).unwrap();
        match self.filestream.get_ref().set_len(remove_from).await {
            Ok(_) => (),
            Err(_) => return Err(LogError::TruncateFailure(remove_from)),
        }
        match self.filestream.get_ref().sync_all().await {
            Ok(_) => (),
            Err(_) => return Err(LogError::SyncFailure),
        }
        self.index.truncate(num_entries + 1);
        Ok(())
    }
}

#[cfg(test)]
mod persistance_tests {
    use std::mem::ManuallyDrop;

    use crate::data::KeyValCommand as Command;
    use rand::{prelude::ThreadRng, thread_rng, Rng};
    type Entry = crate::data::Entry<Command>;

    fn get_random_shm_name(rng: &mut ThreadRng) -> String {
        String::from(format!("/dev/shm/test_log{}", rng.gen::<u64>()))
    }

    fn get_random_entries(rng: &mut ThreadRng, term: u64, num_entries: usize) -> Vec<Entry> {
        let mut entries = Vec::new();
        for index in 0..(num_entries as u64) {
            entries.push(Entry {
                term,
                index,
                command: Command {
                    key: rng.gen::<u32>(),
                    value: rng.gen::<u64>(),
                },
            })
        }
        entries
    }

    #[tokio::test]
    async fn append_and_read() {
        let mut rng = thread_rng();
        let filename = get_random_shm_name(&mut rng);
        let mut logger = super::DurableLog::new(&filename).await.unwrap();
        let write_value = get_random_entries(&mut rng, 1, 3);

        logger.append_entries(&write_value).await.unwrap();
        let read_value = logger.read_entries(1, 5).await.unwrap();
        assert_eq!(read_value, write_value[1..]);

        let read_value = logger.read_entries(0, 1).await.unwrap();
        assert_eq!(read_value, write_value[..=0]);

        assert_eq!(3, logger.total_entries());
        std::fs::remove_file(&filename).unwrap();
    }

    #[tokio::test]
    async fn simple_rewrites() {
        let mut rng = thread_rng();
        let filename = get_random_shm_name(&mut rng);
        let mut logger = super::DurableLog::new(&filename).await.unwrap();
        let write_value = get_random_entries(&mut rng, 1, 3);

        logger.append_entries(&write_value).await.unwrap();
        assert_eq!(3, logger.total_entries());
        let read_value = logger.read_entries(1, 100).await.unwrap();
        assert_eq!(write_value[1..], read_value);

        let write_value = get_random_entries(&mut rng, 2, 3);
        logger.remove_entries_from(0).await.unwrap();
        logger.append_entries(&write_value).await.unwrap();
        assert_eq!(3, logger.total_entries());
        let read_value = logger.read_entries(0, 100).await.unwrap();
        assert_eq!(write_value, read_value);

        std::fs::remove_file(&filename).unwrap();
    }

    #[tokio::test]
    async fn write_and_remove_entries() {
        let mut rng = thread_rng();
        let filename = get_random_shm_name(&mut rng);
        let mut logger = super::DurableLog::new(&filename).await.unwrap();
        let write_value = get_random_entries(&mut rng, 1, 3);

        logger.append_entries(&write_value).await.unwrap();
        assert_eq!(3, logger.total_entries());

        logger.remove_entries_from(1).await.unwrap();
        assert_eq!(1, logger.total_entries());

        let write_value_second = get_random_entries(&mut rng, 2, 4);
        logger.append_entries(&write_value_second).await.unwrap();
        assert_eq!(5, logger.total_entries());

        let write_value_third = get_random_entries(&mut rng, 3, 6);
        logger.remove_entries_from(3).await.unwrap();
        logger.append_entries(&write_value_third).await.unwrap();
        assert_eq!(9, logger.total_entries());

        let read_value = logger.read_entries(0, usize::MAX).await.unwrap();
        assert_eq!(read_value.len(), 9);

        assert_eq!(write_value[0], read_value[0]);
        assert_eq!(write_value_second[..2], read_value[1..3]);
        assert_eq!(write_value_third, read_value[3..]);

        std::fs::remove_file(&filename).unwrap();
    }

    #[tokio::test]
    async fn test_indexing() {
        let mut rng = thread_rng();
        let filename = get_random_shm_name(&mut rng);
        let logger = super::DurableLog::new(&filename).await.unwrap();
        let mut logger = ManuallyDrop::new(logger);
        let write_value = get_random_entries(&mut rng, 1, 3);

        logger.append_entries(&write_value).await.unwrap();
        drop(logger);

        let logger = super::DurableLog::<Entry>::new(&filename).await.unwrap();
        let mut logger = ManuallyDrop::new(logger);
        let read_value = logger.read_entries(1, 6).await.unwrap();
        assert_eq!(read_value, write_value[1..]);

        assert_eq!(3, logger.total_entries());
        std::fs::remove_file(&filename).unwrap();
    }
}
