# chardonnay

The World's Best Transactional Database System.

A research system pushing the boundaries of what's possible in OLTP systems in modern datacenters.

## Publications

Chardonnay: Fast and General Datacenter Transactions for On-Disk Databases, OSDI 2023
https://www.usenix.org/system/files/osdi23-eldeeb.pdf

Chablis: Fast and General Transactions in Geo-Distributed Systems, CIDR 2024 (Best Paper Award!)
https://www.cidrdb.org/cidr2024/papers/p4-eldeeb_v2.pdf

## Building

### Dev Dependencies

**Building Chardonnay requires:**

- Rust toolchain
- Flatbuf compiler aka `flatc`
- Protocol Buffer compiler aka `protoc`

On **macOS** you can install `flatc` and `protoc` using homebrew:
```sh
brew install flatbuffers
brew install protobuf
```

On **Linux**, the versions of `flatc` and `protoc` provided by many distros are
very old. Install them from source or their official release binaries instead.

- https://github.com/google/flatbuffers/releases
- https://github.com/protocolbuffers/protobuf/releases

**Testing requires:**
- Docker

### Build

From the project root:

```
cargo build
```

## Testing

### Setup Environment

To run the tests you need:

- A Cassandra cluster running on port 9042 with the schema loaded.

1. Install the [dev dependencies](#dev-dependencies).

1. Start the Cassandra server:

   ```sh
   docker run -d -p 9042:9042 --name cassandra cassandra:5.0
   ```

1. Load the Chardonnay schema:

   ```sh
   docker exec -i cassandra cqlsh < schema/cassandra/chardonnay/keyspace.cql
   docker exec -i cassandra cqlsh -k chardonnay < schema/cassandra/chardonnay/schema.cql
   ```

### Run Tests

Run:

```sh
cargo test
```

## Building Chardonnay with Docker

Run:

```sh
RANGESERVER_IMG="chardonnay-rangeserver"
WARDEN_IMG="chardonnay-warden"
EPOCH_PUBLISHER_IMG="chardonnay-epoch-publisher"
EPOCH_IMG="chardonnay-epoch"
UNIVERSE_IMG="chardonnay-universe"

TAG="latest"

docker build -t "$RANGESERVER_IMG:$TAG" --target rangeserver .
docker build -t "$WARDEN_IMG:$TAG" --target warden .
docker build -t "$EPOCH_PUBLISHER_IMG:$TAG" --target epoch_publisher .
docker build -t "$EPOCH_IMG:$TAG" --target epoch .
docker build -t "$UNIVERSE_IMG:$TAG" --target universe .
```

## Running Chardonnay on Kubernetes

Prerequisites:
- Minikube installation

:warning: Currently, the Kubernetes deployment in anticipation of the universe manager.


1. Start minikube:

   ```sh
   minikube start
   ```

2. Load chardonnay docker images on minikube:

   ```sh
   minikube image load --overwrite "$RANGESERVER_IMG:$TAG"
   minikube image load --overwrite "$WARDEN_IMG:$TAG"
   minikube image load --overwrite "$EPOCH_PUBLISHER_IMG:$TAG"
   minikube image load --overwrite "$EPOCH_IMG:$TAG"
   minikube image load --overwrite "$UNIVERSE_IMG:$TAG"
   ```

   :note: You might need to delete and re-load the images:

   ```sh
   minikube image rm "$RANGESERVER_IMG:$TAG"
   minikube image rm "$WARDEN_IMG:$TAG"
   minikube image rm "$EPOCH_PUBLISHER_IMG:$TAG"
   minikube image rm "$EPOCH_IMG:$TAG"
   minikube image rm "$UNIVERSE_IMG:$TAG"
   ```

3. Apply chardonnay manifests for deploying on Kubernetes:

   ```sh
   kubectl apply \
      -f kubernetes/namespace.yaml \
      -f kubernetes/cassandra.yaml \
      -f kubernetes/rangeserver.yaml \
      -f kubernetes/warden.yaml \
      -f kubernetes/epoch_publisher.yaml \
      -f kubernetes/epoch_service.yaml \
      -f kubernetes/universe.yaml
   ```

   :warning: Many components of Chardonnay currently crash when their
   dependencies are not available yet, instead of simply retrying. This means
   you may need to wait for some minutes for the deployment to stabilize.

4. Exec into the cassandra container and create the necessary keyspace and
   schema:

   ```sh
   kubectl exec -it -n chardonnay cassandra-0 -- bash

   # Open a cql shell
   cqlsh
   # Copy paste the commands from keyspace.cql
   USE chardonnay;
   # Copy paste the commands from schema.cql
   ```

5. Confirm that everything becomes ready:

   ```sh
   kubectl get pods -n chardonnay
   ```
