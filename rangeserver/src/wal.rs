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
    type Entry;

    async fn first_offset(&self) -> Result<u64, Error>;
    async fn next_offset(&self) -> Result<u64, Error>;
    async fn append(&mut self, entry: Self::Entry) -> Result<(), Error>;
    async fn trim_before_offset(&mut self, offset: u64) -> Result<(), Error>;
    fn iterator(&self) -> impl Iterator;
}

struct InMemoryWal {
    first_offset: u64,
    entries: VecDeque<LogEntry>,
}

struct InMemIterator<'a> {
    index: u64,
    wal: &'a InMemoryWal,
}

impl<'a> Iterator for InMemIterator<'a> {
    type Entry = LogEntry;
    async fn next_offset(&self) -> Result<u64, Error> {
        let offset = self.wal.first_offset + self.index;
        Ok(offset)
    }

    async fn next(&mut self) -> Option<&Self::Entry> {
        let ind = (self.wal.first_offset + self.index) as usize;
        if ind >= self.wal.entries.len() {
            return None;
        }
        self.wal.entries.get(ind)
    }
}

impl Wal for InMemoryWal {
    type Entry = LogEntry;

    async fn first_offset(&self) -> Result<u64, Error> {
        Ok(self.first_offset)
    }

    async fn next_offset(&self) -> Result<u64, Error> {
        let len_u64 = self.entries.len() as u64;
        Ok(self.first_offset + len_u64)
    }

    async fn append(&mut self, entry: Self::Entry) -> Result<(), Error> {
        self.entries.push_back(entry);
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
        }
    }
}
