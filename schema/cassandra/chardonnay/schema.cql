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

CREATE TABLE versioned_records (
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

CREATE TABLE records (
    range_id           uuid,
    key                blob,
    value              blob,
    epoch              bigint,
    PRIMARY KEY  ((range_id), key)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };