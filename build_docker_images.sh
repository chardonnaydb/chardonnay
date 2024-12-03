RANGESERVER_IMG="chardonnay-rangeserver"
WARDEN_IMG="chardonnay-warden"
EPOCH_PUBLISHER_IMG="chardonnay-epoch-publisher"
EPOCH_IMG="chardonnay-epoch"
UNIVERSE_IMG="chardonnay-universe"
CASSANDRA_CLIENT_IMG="chardonnay-cassandra-client"

TAG="latest"

docker build -t "$RANGESERVER_IMG:$TAG" --target rangeserver .
docker build -t "$WARDEN_IMG:$TAG" --target warden .
docker build -t "$EPOCH_PUBLISHER_IMG:$TAG" --target epoch_publisher .
docker build -t "$EPOCH_IMG:$TAG" --target epoch .
docker build -t "$UNIVERSE_IMG:$TAG" --target universe .
docker build -t "$CASSANDRA_CLIENT_IMG:$TAG" --target cassandra-client .
