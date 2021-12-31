pub use super::common::CommandType as Command;
pub use super::common::EntryType as Entry;
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

const ENTRY_SIZE: usize = std::mem::size_of::<Entry>();
type Error = Box<dyn std::error::Error>;

unsafe fn as_u8_slice<T: Sized>(object: &T) -> &[u8] {
    std::slice::from_raw_parts((object as *const T) as *const u8, std::mem::size_of::<T>())
}

struct PersistanceLog {
    file: tokio::fs::File,
    total_entries: usize,
}

impl PersistanceLog {
    async fn new(filename: &str) -> Result<PersistanceLog, Error> {
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(filename)
            .await?;
        let metadata = file.metadata().await?;
        Ok(PersistanceLog {
            file,
            total_entries: (metadata.len() / ENTRY_SIZE as u64) as usize,
        })
    }

    fn total_entries(&self) -> usize {
        self.total_entries
    }
    async fn append_entries(&mut self, entries: Vec<Entry>) -> Result<(), Error> {
        self.file
            .seek(std::io::SeekFrom::Start(
                (self.total_entries * ENTRY_SIZE) as u64,
            ))
            .await?;
        for entry in entries.iter() {
            self.file.write_all(unsafe { as_u8_slice(entry) }).await?;
        }
        self.file.sync_all().await?;
        self.total_entries += entries.len();
        Ok(())
    }
    async fn write_entries(&mut self, index: usize, entries: Vec<Entry>) -> Result<(), Error> {
        let write_index = std::cmp::min(self.total_entries, index);
        self.file
            .seek(std::io::SeekFrom::Start((write_index * ENTRY_SIZE) as u64))
            .await?;
        for entry in entries.iter() {
            self.file.write_all(unsafe { as_u8_slice(entry) }).await?;
        }
        self.file.sync_all().await?;
        self.total_entries = std::cmp::max(write_index + entries.len(), self.total_entries);
        Ok(())
    }
    async fn read_entries(&mut self, index: usize, count: usize) -> Result<Vec<Entry>, Error> {
        self.file
            .seek(std::io::SeekFrom::Start(
                (std::cmp::min(self.total_entries, index) * ENTRY_SIZE) as u64,
            ))
            .await?;
        let read_count = std::cmp::min(count, self.total_entries - index);
        let mut buffer = Vec::new();
        buffer.resize(ENTRY_SIZE * read_count, 0u8);
        self.file.read_exact(&mut buffer[..]).await?;
        let entries = unsafe {
            let mut buf = std::mem::ManuallyDrop::new(buffer);
            Vec::from_raw_parts(buf.as_mut_ptr() as *mut Entry, read_count, read_count)
        };
        Ok(entries)
    }

    async fn remove_last_entries(&mut self, index: usize) -> Result<(), Error> {
        self.total_entries = std::cmp::min(self.total_entries, index);
        self.file
            .set_len(ENTRY_SIZE as u64 * self.total_entries as u64)
            .await?;
        self.file.sync_all().await?;
        Ok(())
    }
}

#[cfg(test)]
mod persistance_tests {
    use super::Command;
    use super::Entry;
    use super::Error;

    #[tokio::test]
    async fn append_and_read() -> Result<(), Error> {
        let mut logger = super::PersistanceLog::new("/dev/shm/append_and_read_log").await?;
        let write_value = vec![Entry {
            term: 0,
            index: 0,
            command: Command {
                key: 214227,
                value: 219472189,
            },
        }];
        logger.append_entries(write_value.clone()).await?;
        let read_value = logger.read_entries(0, 100).await?;
        assert_eq!(read_value, write_value);
        assert_eq!(1, logger.total_entries());
        Ok(())
    }

    #[tokio::test]
    async fn write_and_read() -> Result<(), Error> {
        let mut logger = super::PersistanceLog::new("/dev/shm/append_and_read_log").await?;
        let write_value = vec![Entry {
            term: 0,
            index: 0,
            command: Command {
                key: 214227,
                value: 219472189,
            },
        }];
        logger.append_entries(write_value.clone()).await?;
        let read_value = logger.read_entries(0, 100).await?;
        assert_eq!(read_value, write_value);
        assert_eq!(1, logger.total_entries());
        Ok(())
    }
}
