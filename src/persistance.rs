use crate::data::as_u8_slice;
use std::marker::PhantomData;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter};

type Error = Box<dyn std::error::Error>;
struct TypedLog<Entry> {
    filestream: BufWriter<tokio::fs::File>,
    total_entries: usize,
    dummy: PhantomData<Entry>,
}

impl<Entry> TypedLog<Entry> {
    const ENTRY_SIZE: usize = std::mem::size_of::<Entry>();
    async fn new(filename: &str) -> Result<TypedLog<Entry>, Error> {
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(filename)
            .await?;
        let metadata = file.metadata().await?;

        Ok(TypedLog {
            filestream: BufWriter::new(file),
            total_entries: (metadata.len() / Self::ENTRY_SIZE as u64) as usize,
            dummy: PhantomData,
        })
    }

    fn total_entries(&self) -> usize {
        self.total_entries
    }

    async fn append_entries(&mut self, entries: &Vec<Entry>) -> Result<usize, Error> {
        self.filestream.seek(std::io::SeekFrom::End(0)).await?;
        for entry in entries.iter() {
            self.filestream
                .write_all(unsafe { as_u8_slice(entry) })
                .await?;
        }
        self.filestream.flush().await?;
        self.filestream.get_ref().sync_all().await?;
        self.total_entries += entries.len();
        Ok(self.total_entries)
    }

    async fn read_entries(&mut self, index: usize, count: usize) -> Result<Vec<Entry>, Error> {
        let seek_index = std::cmp::min(self.total_entries, index);
        self.filestream
            .seek(std::io::SeekFrom::Start(
                (seek_index * Self::ENTRY_SIZE) as u64,
            ))
            .await?;
        let read_count = std::cmp::min(count, self.total_entries - seek_index);
        let mut buffer = Vec::new();
        buffer.resize(Self::ENTRY_SIZE * read_count, 0u8);
        self.filestream.read_exact(&mut buffer[..]).await?;
        let entries = unsafe {
            let mut buf = std::mem::ManuallyDrop::new(buffer);
            Vec::from_raw_parts(buf.as_mut_ptr() as *mut Entry, read_count, read_count)
        };
        Ok(entries)
    }

    async fn remove_entries_from(&mut self, index: usize) -> Result<(), Error> {
        self.total_entries = std::cmp::min(self.total_entries, index);
        self.filestream
            .set_len(Self::ENTRY_SIZE as u64 * self.total_entries as u64)
            .await?;
        self.filestream.flush().await?;
        self.filestream.get_ref().sync_all().await?;
        Ok(())
    }
}

#[cfg(test)]
mod persistance_tests {
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
        let mut logger = super::TypedLog::new(&filename).await.unwrap();
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
        let mut logger = super::TypedLog::new(&filename).await.unwrap();
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
        let mut logger = super::TypedLog::new(&filename).await.unwrap();
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
}
