use std::collections::VecDeque;

use flatbuff::rangeserver_flatbuffers::range_server::*;

enum Error {
    Unknown,
}

trait Iterator {
    type Entry;
    async fn next(&mut self) -> Option<&Self::Entry>;
    async fn next_offset(&self) -> Result<u64, Error>;
}

pub trait Wal {
    async fn first_offset(&self) -> Result<u64, Error>;
    async fn next_offset(&self) -> Result<u64, Error>;
    async fn append(&mut self, entry: LogEntry<'_>) -> Result<(), Error>;
    async fn trim_before_offset(&mut self, offset: u64) -> Result<(), Error>;
    fn iterator(&self) -> impl Iterator;
}

struct InMemoryWal {
    first_offset: u64,
    entries: VecDeque<Vec<u8>>,
}

struct InMemIterator<'a> {
    index: u64,
    wal: &'a InMemoryWal,
    current_entry: Option<LogEntry<'a>>,
}

impl<'a> Iterator for InMemIterator<'a> {
    type Entry = LogEntry<'a>;
    async fn next_offset(&self) -> Result<u64, Error> {
        let offset = self.wal.first_offset + self.index;
        Ok(offset)
    }

    async fn next(&mut self) -> Option<&Self::Entry> {
        let ind = (self.wal.first_offset + self.index) as usize;
        if ind >= self.wal.entries.len() {
            return None;
        }
        self.current_entry = Some(root_as_log_entry(self.wal.entries.get(ind).unwrap()).unwrap());
        match &self.current_entry {
            None => None,
            Some(e) => Some(e),
        }
    }
}

impl Wal for InMemoryWal {
    async fn first_offset(&self) -> Result<u64, Error> {
        Ok(self.first_offset)
    }

    async fn next_offset(&self) -> Result<u64, Error> {
        let len_u64 = self.entries.len() as u64;
        Ok(self.first_offset + len_u64)
    }

    async fn append(&mut self, entry: LogEntry<'_>) -> Result<(), Error> {
        let buf = Vec::from(entry._tab.buf());
        self.entries.push_back(buf);
        Ok(())
    }

    async fn trim_before_offset(&mut self, offset: u64) -> Result<(), Error> {
        while self.entries.len() > 0 && self.first_offset < offset {
            self.entries.pop_front();
            self.first_offset += 1;
        }
        Ok(())
    }

    fn iterator<'a>(&'a self) -> InMemIterator<'a> {
        InMemIterator {
            index: 0,
            wal: self,
            current_entry: None,
        }
    }
}
