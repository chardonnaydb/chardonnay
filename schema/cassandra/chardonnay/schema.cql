CREATE TABLE range_map (
    keyspace_id                  uuid,
    range_id                     uuid,
    key_lower_bound_inclusive    blob,
    key_upper_bound_exclusive    blob,
    assignee                     text,
    PRIMARY KEY  ((keyspace_id), range_id)
  ) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };

CREATE TABLE epoch (
    region      text,
    epoch       bigint,
    timestamp   timestamp, 
    PRIMARY KEY  (region)
  ) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };

CREATE TYPE epoch_range (
  lower_bound_inclusive    bigint,
  upper_bound_inclusive    bigint
);

CREATE TABLE range_leases (
  range_id                     uuid,
  key_lower_bound_inclusive    blob,
  key_upper_bound_exclusive    blob,
  leader_sequence_number       bigint,
  epoch_lease                  epoch_range,
  safe_snapshot_epochs         epoch_range,
  PRIMARY KEY  (range_id)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };

CREATE TABLE records (
    range_id           uuid,
    key                blob,
    epoch              bigint,
    value              blob,
    is_tombstone       boolean,
    PRIMARY KEY  ((range_id), key, epoch)
) WITH CLUSTERING ORDER BY (key ASC, epoch DESC)
  AND COMPACTION = {
     'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
};

CREATE TABLE wal (
    wal_id          uuid,
    first_offset    bigint,
    next_offset     bigint,
    offset          bigint,
    write_id        uuid,
    content         blob,
    PRIMARY KEY  ((wal_id), offset)
) WITH CLUSTERING ORDER BY (offset ASC)
  AND COMPACTION = {
     'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
};

CREATE TABLE transactions (
    transaction_id    uuid,
    status            text,
    epoch             bigint,
    commit_info       blob,
    PRIMARY KEY  (transaction_id)
) WITH COMPACTION = {
     'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
};