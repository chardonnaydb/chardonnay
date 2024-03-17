# chardonnay

The World's Best Transactional Database System.

## Testing

### Setup Environment

To run the tests you need:
- A Cassandra cluster running on port 9042 with the schema loaded.

1. Install the necessary packages:

    ```sh
    brew install cassandra docker flatbuffers
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