cqlsh cassandra 9042 -f /etc/chardonnay/cassandra/keyspace.cql
cqlsh cassandra 9042 -k chardonnay -f /etc/chardonnay/cassandra/schema.cql
tail -f /dev/null
