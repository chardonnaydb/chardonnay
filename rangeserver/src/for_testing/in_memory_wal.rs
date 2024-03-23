use crate::wal::*;

use std::collections::VecDeque;

use flatbuf::rangeserver_flatbuffers::range_server::*;
use flatbuffers::FlatBufferBuilder;

pub struct InMemoryWal {
    first_offset: u64,
    entries: VecDeque<Vec<u8>>,
    flatbuf_builder: FlatBufferBuilder<'static>,
}

struct InMemIterator<'a> {
    index: u64,
    wal: &'a InMemoryWal,
    current_entry: Option<LogEntry<'a>>,
}

impl<'a> Iterator<'a> for InMemIterator<'a> {
    async fn next_offset(&self) -> Result<u64, Error> {
        let offset = self.wal.first_offset + self.index;
        Ok(offset)
    }

    async fn next<'b>(&'b mut self) -> Option<&LogEntry<'b>> {
        let ind = (self.wal.first_offset + self.index) as usize;
        if ind >= self.wal.entries.len() {
            return None;
        }
        self.current_entry = Some(root_as_log_entry(self.wal.entries.get(ind).unwrap()).unwrap());
        self.index += 1;
        match &self.current_entry {
            None => None,
            Some(e) => Some(e),
        }
    }
}

impl InMemoryWal {
    pub fn new() -> Self {
        InMemoryWal {
            first_offset: 0,
            entries: VecDeque::new(),
            flatbuf_builder: FlatBufferBuilder::new(),
        }
    }
    fn append_data_currently_in_builder(&mut self) -> Result<(), Error> {
        let bytes = self.flatbuf_builder.finished_data();
        let buf = Vec::from(bytes);
        self.entries.push_back(buf);
        self.flatbuf_builder.reset();
        Ok(())
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

    async fn trim_before_offset(&mut self, offset: u64) -> Result<(), Error> {
        while self.entries.len() > 0 && self.first_offset < offset {
            self.entries.pop_front();
            self.first_offset += 1;
        }
        Ok(())
    }

    async fn append_prepare(&mut self, entry: PrepareRequest<'_>) -> Result<(), Error> {
        let prepare_bytes = self.flatbuf_builder.create_vector(entry._tab.buf());
        let fb_root = LogEntry::create(
            &mut self.flatbuf_builder,
            &LogEntryArgs {
                entry: Entry::Prepare,
                bytes: Some(prepare_bytes),
            },
        );
        self.flatbuf_builder.finish(fb_root, None);
        self.append_data_currently_in_builder()
    }

    async fn append_commit(&mut self, entry: CommitRequest<'_>) -> Result<(), Error> {
        let commit_bytes = self.flatbuf_builder.create_vector(entry._tab.buf());
        let fb_root = LogEntry::create(
            &mut self.flatbuf_builder,
            &LogEntryArgs {
                entry: Entry::Commit,
                bytes: Some(commit_bytes),
            },
        );
        self.flatbuf_builder.finish(fb_root, None);
        self.append_data_currently_in_builder()
    }

    async fn append_abort(&mut self, entry: AbortRequest<'_>) -> Result<(), Error> {
        let abort_bytes = self.flatbuf_builder.create_vector(entry._tab.buf());
        let fb_root = LogEntry::create(
            &mut self.flatbuf_builder,
            &LogEntryArgs {
                entry: Entry::Commit,
                bytes: Some(abort_bytes),
            },
        );
        self.flatbuf_builder.finish(fb_root, None);
        self.append_data_currently_in_builder()
    }

    fn iterator<'a>(&'a self) -> InMemIterator<'a> {
        InMemIterator {
            index: 0,
            wal: self,
            current_entry: None,
        }
    }
}
