# chardonnay

The World's Best Transactional Database System.

A research system pushing the boundaries of what's possible in OLTP systems in modern datacenters.

## Publications

Chardonnay: Fast and General Datacenter Transactions for On-Disk Databases, OSDI 2023
https://www.usenix.org/system/files/osdi23-eldeeb.pdf

Chablis: Fast and General Transactions in Geo-Distributed Systems, CIDR 2024 (Best Paper Award!)
https://www.cidrdb.org/cidr2024/papers/p4-eldeeb.pdf

## Building

### Dependencies

Building Chardonnay requires:

- Rust toolchain
- Flatbuf compiler aka `flatc` (On OSX, `brew install flatbuffers`)
- Protocol Buffer compiler aka `protoc` (On OSX, `brew install protobuf`)

### Build

From the project root:

```
cargo build
```

## Testing

### Setup Environment

To run the tests you need:

- A Cassandra cluster running on port 9042 with the schema loaded.

1. Install the necessary packages:

   ```sh
   brew install cassandra docker flatbuffers protobuf
   ```

1. Start the Cassandra server:

   ```sh
   docker run -d -p 9042:9042 --name cassandra cassandra:5.0
   ```

1. Load the Chardonnay schema:

   ```sh
   cqlsh -f schema/cassandra/chardonnay/keyspace.cql
   cqlsh -k chardonnay -f schema/cassandra/chardonnay/schema.cql
   ```

### Run Tests

Run:

```sh
cargo test
```
