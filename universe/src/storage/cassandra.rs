use std::str::FromStr;

use super::*;
use proto::universe::KeyspaceInfo;
use scylla::macros::{FromUserType, SerializeValue};
use scylla::query::Query;
use scylla::statement::SerialConsistency;
use scylla::transport::errors::DbError;
use scylla::transport::errors::QueryError;
use scylla::transport::PagingState;
use scylla::Session;
use scylla::{FromRow, SerializeRow, SessionBuilder};
use uuid::Uuid;

pub struct Cassandra {
    session: Session,
}

static CREATE_KEYSPACE_QUERY: &str = r#"
    INSERT INTO chardonnay.keyspaces
    (keyspace_id, name, namespace, primary_zone, base_key_ranges)
    VALUES (?, ?, ?, ?, ?)
"#;

static LIST_KEYSPACES_QUERY: &str = r#"
    SELECT keyspace_id, name, namespace, primary_zone, base_key_ranges
    FROM chardonnay.keyspaces
"#;

// TODO: Similar to tx_state_store. We should move this to a common location.
fn get_serial_query(query_text: impl Into<String>) -> Query {
    let mut query = Query::new(query_text);
    query.set_serial_consistency(Some(SerialConsistency::Serial));
    query
}

// TODO: Similar to rangeserver and tx_state_store. We should move this to a
// common location.
fn scylla_query_error_to_storage_error(qe: QueryError) -> Error {
    match qe {
        QueryError::TimeoutError | QueryError::DbError(DbError::WriteTimeout { .. }, _) => {
            Error::Timeout
        }
        _ => Error::InternalError(Arc::new(qe)),
    }
}

// Wrapper struct for the KeyspaceInfo proto.
// This is needed because the KeyspaceInfo proto is not directly convertible to
// a Scylla row. Instead, we need to implement the FromRow trait.
#[derive(Debug)]
struct KeyspaceInfoWrapper(KeyspaceInfo);

#[derive(Debug, FromUserType, SerializeValue)]
struct SerializedRegion {
    name: String,
    cloud: Option<String>,
}

#[derive(Debug, FromUserType, SerializeValue)]
struct SerializedZone {
    region: Option<SerializedRegion>,
    name: String,
}

#[derive(Debug, FromUserType, SerializeValue)]
struct SerializedKeyRange {
    base_range_uuid: Uuid,
    lower_bound_inclusive: Vec<u8>,
    upper_bound_exclusive: Vec<u8>,
}

#[derive(Debug, FromRow, SerializeRow)]
struct SerializedKeyspaceInfo {
    keyspace_id: Uuid,
    name: String,
    namespace: String,
    primary_zone: Option<SerializedZone>,
    // We need Option here because Scylla doesn't support empty lists.
    // Without Option, on reading, deserialization will fail if the list is empty.
    base_key_ranges: Option<Vec<SerializedKeyRange>>,
}

impl SerializedKeyspaceInfo {
    fn construct_from_parts(
        name: String,
        namespace: String,
        primary_zone: Option<Zone>,
        base_key_range_requests: Vec<KeyRangeRequest>,
    ) -> Self {
        SerializedKeyspaceInfo {
            keyspace_id: Uuid::new_v4(),
            name: name.to_string(),
            namespace: namespace.to_string(),
            primary_zone: primary_zone.map(|zone| SerializedZone {
                region: zone.region.map(|region| SerializedRegion {
                    name: region.name,
                    cloud: region.cloud.map(|cloud| match cloud {
                        proto::universe::region::Cloud::PublicCloud(cloud) => {
                            proto::universe::Cloud::try_from(cloud)
                                .map(|c| c.as_str_name().to_string())
                                .unwrap_or_default()
                        }
                        proto::universe::region::Cloud::OtherCloud(other) => other,
                    }),
                }),
                name: zone.name,
            }),
            base_key_ranges: if base_key_range_requests.is_empty() {
                None
            } else {
                Some(
                    base_key_range_requests
                        .into_iter()
                        .map(|range| SerializedKeyRange {
                            base_range_uuid: Uuid::new_v4(),
                            lower_bound_inclusive: range.lower_bound_inclusive,
                            upper_bound_exclusive: range.upper_bound_exclusive,
                        })
                        .collect(),
                )
            },
        }
    }

    /// Convert into a KeyspaceInfo proto.
    fn to_keyspace_info(self) -> KeyspaceInfo {
        let primary_zone = self.primary_zone.map(|zone| Zone {
            name: zone.name,
            region: zone.region.map(|region| proto::universe::Region {
                name: region.name,
                cloud: region.cloud.map(|cloud| match cloud.as_str() {
                    "AWS" => proto::universe::region::Cloud::PublicCloud(
                        proto::universe::Cloud::Aws as i32,
                    ),
                    "AZURE" => proto::universe::region::Cloud::PublicCloud(
                        proto::universe::Cloud::Azure as i32,
                    ),
                    "GCP" => proto::universe::region::Cloud::PublicCloud(
                        proto::universe::Cloud::Gcp as i32,
                    ),
                    other => proto::universe::region::Cloud::OtherCloud(other.to_string()),
                }),
            }),
        });
        let base_key_ranges = self
            .base_key_ranges
            .unwrap_or_default()
            .into_iter()
            .map(|range| KeyRange {
                base_range_uuid: range.base_range_uuid.to_string(),
                lower_bound_inclusive: range.lower_bound_inclusive,
                upper_bound_exclusive: range.upper_bound_exclusive,
            })
            .collect();
        KeyspaceInfo {
            keyspace_id: self.keyspace_id.to_string(),
            name: self.name,
            namespace: self.namespace,
            primary_zone,
            base_key_ranges,
        }
    }
}

impl Cassandra {
    pub async fn new(known_node: String) -> Self {
        let session = SessionBuilder::new()
            .known_node(known_node)
            .build()
            .await
            .unwrap();
        Self { session }
    }
}

impl Storage for Cassandra {
    async fn create_keyspace(
        &self,
        name: &str,
        namespace: &str,
        primary_zone: Option<Zone>,
        base_key_ranges: Vec<KeyRangeRequest>,
    ) -> Result<String, Error> {
        // TODO: Validate base_key_ranges

        // Create a SerializedKeyspaceInfo from the input parameters
        let serialized_info = SerializedKeyspaceInfo::construct_from_parts(
            name.to_string(),
            namespace.to_string(),
            primary_zone,
            base_key_ranges,
        );

        let keyspace_id = serialized_info.keyspace_id.to_string();

        // Execute the query
        let query = get_serial_query(CREATE_KEYSPACE_QUERY);
        self.session
            .query_single_page(query, serialized_info, PagingState::start())
            .await
            .map_err(scylla_query_error_to_storage_error)?;

        Ok(keyspace_id)
    }

    async fn list_keyspaces(&self) -> Result<Vec<KeyspaceInfo>, Error> {
        // Execute the query
        let query = get_serial_query(LIST_KEYSPACES_QUERY);
        let rows = self
            .session
            .query_unpaged(query, ())
            .await
            .map_err(scylla_query_error_to_storage_error)?
            .rows
            .unwrap_or_default();

        // Convert the rows to KeyspaceInfo objects
        let keyspaces = rows
            .into_iter()
            .map(|row| {
                let serialized = row.into_typed::<SerializedKeyspaceInfo>();
                serialized
                    .map(SerializedKeyspaceInfo::to_keyspace_info)
                    .map_err(|e| Error::DeserializationError(Arc::new(e)))
            })
            .collect::<Result<Vec<KeyspaceInfo>, Error>>()?;

        Ok(keyspaces)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::universe::{region, Cloud, KeyRange, KeyspaceInfo, Region, Zone};

    fn create_example_keyspace_info() -> KeyspaceInfo {
        KeyspaceInfo {
            keyspace_id: Uuid::new_v4().to_string(),
            namespace: "example_namespace".to_string(),
            name: "example_keyspace".to_string(),
            primary_zone: Some(Zone {
                region: Some(Region {
                    name: "example_region".to_string(),
                    cloud: Some(region::Cloud::PublicCloud(Cloud::Aws as i32)),
                }),
                name: "example_zone".to_string(),
            }),
            base_key_ranges: vec![
                KeyRange {
                    base_range_uuid: Uuid::new_v4().to_string(),
                    lower_bound_inclusive: vec![0, 0, 0],
                    upper_bound_exclusive: vec![128, 0, 0],
                },
                KeyRange {
                    base_range_uuid: Uuid::new_v4().to_string(),
                    lower_bound_inclusive: vec![128, 0, 0],
                    upper_bound_exclusive: vec![255, 255, 255],
                },
            ],
        }
    }

    fn keyspace_info_equals(a: &KeyspaceInfo, b: &KeyspaceInfo) -> bool {
        a.namespace == b.namespace
            && a.name == b.name
            && a.primary_zone == b.primary_zone
            && a.base_key_ranges.len() == b.base_key_ranges.len()
            && a.base_key_ranges
                .iter()
                .zip(b.base_key_ranges.iter())
                .all(|(a_range, b_range)| {
                    a_range.lower_bound_inclusive == b_range.lower_bound_inclusive
                        && a_range.upper_bound_exclusive == b_range.upper_bound_exclusive
                })
    }

    #[tokio::test]
    async fn test_local_roundtrip() {
        let original = create_example_keyspace_info();
        let serialized = SerializedKeyspaceInfo::construct_from_parts(
            original.name.clone(),
            original.namespace.clone(),
            original.primary_zone.clone(),
            original
                .base_key_ranges
                .iter()
                .map(|range| KeyRangeRequest {
                    lower_bound_inclusive: range.lower_bound_inclusive.clone(),
                    upper_bound_exclusive: range.upper_bound_exclusive.clone(),
                })
                .collect(),
        );
        let roundtrip = serialized.to_keyspace_info();
        assert_eq!(keyspace_info_equals(&original, &roundtrip), true);
    }

    #[tokio::test]
    async fn test_cassandra_roundtrip() {
        let mut example_keyspaces = vec![create_example_keyspace_info()];
        // Keyspace with empty base_key_ranges
        example_keyspaces.push(KeyspaceInfo {
            keyspace_id: Uuid::new_v4().to_string(),
            namespace: "empty_namespace".to_string(),
            name: "empty_keyspace".to_string(),
            primary_zone: Some(Zone {
                region: Some(Region {
                    cloud: Some(proto::universe::region::Cloud::PublicCloud(
                        proto::universe::Cloud::Aws as i32,
                    )),
                    name: "us-west-2".to_string(),
                }),
                name: "us-west-2a".to_string(),
            }),
            base_key_ranges: vec![],
        });
        // Keyspace with no primary_zone
        example_keyspaces.push(KeyspaceInfo {
            keyspace_id: Uuid::new_v4().to_string(),
            namespace: "no_primary_zone_namespace".to_string(),
            name: "no_primary_zone_keyspace".to_string(),
            primary_zone: None,
            base_key_ranges: vec![],
        });
        // Start a session
        let storage = Cassandra::new("127.0.0.1:9042".to_string()).await;

        for original in example_keyspaces {
            // Insert into Cassandra
            let base_key_range_requests = original
                .base_key_ranges
                .iter()
                .map(|range| KeyRangeRequest {
                    lower_bound_inclusive: range.lower_bound_inclusive.clone(),
                    upper_bound_exclusive: range.upper_bound_exclusive.clone(),
                })
                .collect();
            let keyspace_id = storage
                .create_keyspace(
                    &original.name,
                    &original.namespace,
                    original.primary_zone.clone(),
                    base_key_range_requests,
                )
                .await
                .unwrap();
            // Print keyspace id
            println!("Keyspace ID: {}", keyspace_id);
            // List keyspaces from Cassandra
            let keyspaces = storage.list_keyspaces().await.unwrap();
            // Find the keyspace we inserted by id
            let roundtrip = keyspaces
                .into_iter()
                .find(|k| k.keyspace_id == keyspace_id)
                .unwrap_or_else(|| panic!("Keyspace with id {} was not found", keyspace_id));

            assert_eq!(keyspace_info_equals(&original, &roundtrip), true);
        }
    }

    #[tokio::test]
    async fn test_keyspace_without_base_key_ranges() {
        // Start a session
        let storage = Cassandra::new("127.0.0.1:9042".to_string()).await;
        let original = KeyspaceInfo {
            keyspace_id: Uuid::new_v4().to_string(),
            namespace: "example_namespace".to_string(),
            name: "example_keyspace".to_string(),
            primary_zone: None,
            base_key_ranges: vec![],
        };
        // Insert into Cassandra
        let base_key_range_requests = original
            .base_key_ranges
            .iter()
            .map(|range| KeyRangeRequest {
                lower_bound_inclusive: range.lower_bound_inclusive.clone(),
                upper_bound_exclusive: range.upper_bound_exclusive.clone(),
            })
            .collect();
        let keyspace_id = storage
            .create_keyspace(
                &original.name,
                &original.namespace,
                original.primary_zone.clone(),
                base_key_range_requests,
            )
            .await
            .unwrap();
        // Print keyspace id
        println!("Keyspace ID: {}", keyspace_id);
        // List keyspaces from Cassandra
        let keyspaces = storage.list_keyspaces().await.unwrap();
        // Find the keyspace we inserted by id
        let roundtrip = keyspaces
            .into_iter()
            .find(|k| k.keyspace_id == keyspace_id)
            .unwrap_or_else(|| panic!("Keyspace with id {} was not found", keyspace_id));

        // Fully print original and roundtrip
        println!("Original: {:#?}", original);
        println!("Roundtrip: {:#?}", roundtrip);

        assert_eq!(keyspace_info_equals(&original, &roundtrip), true);
    }
}
