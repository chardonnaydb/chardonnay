use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::{collections::HashSet, ops::Deref, sync::Mutex};

use common::host_info::HostInfo;
use proto::warden::WardenUpdate;
use std::cmp::{Ordering, Reverse};
use std::hash::{Hash, Hasher};
<<<<<<< HEAD
use tokio::sync::broadcast::{channel, Receiver, Sender};
=======
use tokio::sync::broadcast::Receiver;
>>>>>>> origin/main
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::{debug, info};

use crate::persistence::{RangeAssignment, RangeInfo};

// TODO(purujit): Convert this to a configuration.
const MIN_NUM_RANGE_SERVERS: usize = 1;

/// We need to implement Eq, Ord and Hash for Range Server identities in
/// HostInfo.  Since that type is in another crate and may have a different
/// implementation of these traits, we need to wrap it. This is utilizing the
/// `NewType` pattern from chapter 19 of the Rust book.
#[derive(Clone, Debug)]
struct HostInfoWrapper(HostInfo);
impl Deref for HostInfoWrapper {
    type Target = HostInfo;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
<<<<<<< HEAD
}

impl Eq for HostInfoWrapper {}

impl PartialEq for HostInfoWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity
    }
}

impl PartialOrd for HostInfoWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HostInfoWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        self.identity.cmp(&other.identity)
    }
}

impl Hash for HostInfoWrapper {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.identity.hash(state);
    }
}

pub trait AssignmentComputation {
    fn register_range_server(&self, host_info: HostInfo) -> Receiver<i64>;
    fn notify_range_server_unavailable(&self, host_info: HostInfo);
    fn get_assignment_update(
        &self,
        host_info: &HostInfo,
        version: i64,
        full_update: bool,
    ) -> Option<WardenUpdate>;
}

pub struct AssignmentComputationImpl {
    base_ranges: Option<Vec<RangeInfo>>,
    range_assignments: Mutex<Vec<RangeAssignment>>,
    ready_range_servers: Mutex<HashSet<HostInfoWrapper>>,
    unassigned_base_ranges: Mutex<Vec<RangeInfo>>,
    assignment_update_sender: Sender<i64>,
}

=======
}

impl Eq for HostInfoWrapper {}

impl PartialEq for HostInfoWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity
    }
}

impl PartialOrd for HostInfoWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HostInfoWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        self.identity.cmp(&other.identity)
    }
}

impl Hash for HostInfoWrapper {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.identity.hash(state);
    }
}

pub trait AssignmentComputation {
    fn register_range_server(&self, host_info: HostInfo) -> Result<Receiver<i64>, Status>;
    fn get_assignment(&self, version: i64, full_update: bool) -> Option<WardenUpdate>;
}
pub struct AssignmentComputationImpl {
    base_ranges: Option<Vec<RangeInfo>>,
    range_assignments: Mutex<Vec<RangeAssignment>>,
    ready_range_servers: Mutex<HashSet<HostInfoWrapper>>,
    unassigned_base_ranges: Mutex<Vec<RangeInfo>>,
}

>>>>>>> origin/main
impl AssignmentComputationImpl {
    pub fn new(
        runtime: tokio::runtime::Handle,
        cancellation_token: CancellationToken,
    ) -> Arc<Self> {
        let s = Arc::new(Self {
            // TODO(purujit): Initialize base_ranges and range_assignments from database.
            base_ranges: None,
            range_assignments: Mutex::new(vec![]),
            ready_range_servers: Mutex::new(HashSet::new()),
            unassigned_base_ranges: Mutex::new(vec![]),
<<<<<<< HEAD
            // Using capacity 1 here because receivers will resync if they lag.
            assignment_update_sender: channel(1).0,
=======
>>>>>>> origin/main
        });
        let computation_clone = s.clone();
        runtime.spawn(async move {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("Cancellation token triggered. Exiting assignment computation loop.")
                }
                _ = Self::assignment_computation_loop(computation_clone) => {
                    info!("Assignment computation loop exited.")
                }
            }
        });
        s
    }

    async fn assignment_computation_loop(self: Arc<Self>) -> () {
        let mut ready_servers = HashSet::new();
        loop {
            let clone = self.clone();
            ready_servers = clone.run_assignment_computation(ready_servers).await;
        }
    }

    /// Runs the assignment computation loop, which is responsible for managing the assignment of key ranges to range servers.
    ///
    /// This function is called periodically to update the assignment of key ranges to range servers. It performs the following steps:
    ///
    /// 1. Checks if the number of ready range servers is at least the minimum required. If not, it waits for 1 second.
    /// 2. Computes the set of added and removed servers since the last run.
    /// 3. If there are no changes in the set of ready servers or unassigned base ranges, it waits for 1 second.
    /// 4. Constructs a map of assignee (range server) to the list of ranges assigned to that server.
    /// 5. Builds a min-heap of servers, where the priority is the number of ranges assigned to the server.
    /// 6. Reassigns any ranges from removed servers to the current servers, starting from the least loaded.
    /// 7. Assigns any newly added base ranges to the current servers, starting from the least loaded.
    /// 8. Updates the `range_assignments` vector with the new assignments.
    ///
    /// The function returns the current set of ready range servers.
    async fn run_assignment_computation(
        self: Arc<Self>,
        prev_ready_servers: HashSet<HostInfoWrapper>,
    ) -> HashSet<HostInfoWrapper> {
        let new_ready_servers = self.ready_range_servers.lock().unwrap().clone();
        let num_range_servers = new_ready_servers.len();
        if num_range_servers < MIN_NUM_RANGE_SERVERS {
            info!(
                "Need {} range servers at least, got {}. Will wait.",
                MIN_NUM_RANGE_SERVERS, num_range_servers
            );
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            return new_ready_servers;
        }

        let added_servers: Vec<_> = new_ready_servers.difference(&prev_ready_servers).collect();
        let removed_servers: Vec<_> = prev_ready_servers.difference(&new_ready_servers).collect();
        if added_servers.len() == 0
            && removed_servers.len() == 0
            && self.unassigned_base_ranges.lock().unwrap().len() == 0
        {
            debug!("No changes in the set of ready range servers or unassigned base ranges. Will wait.");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            return new_ready_servers;
        }
        debug!(
            "Added servers: {:?}, removed servers: {:?}.",
            added_servers, removed_servers
        );

        let mut assignee_to_range_info = HashMap::new();
        for assignment in self.range_assignments.lock().unwrap().iter() {
            assignee_to_range_info
                .entry(assignment.assignee.clone())
                .or_insert_with(Vec::new)
                .push(assignment.range.clone());
        }

        let mut server_heap = BinaryHeap::new();
        for (server, ranges) in assignee_to_range_info.iter() {
            server_heap.push(Reverse((ranges.len(), server.clone())));
        }
        for added_server in added_servers {
            server_heap.push(Reverse((0, added_server.identity.clone())));
        }

        // TODO(purujit): Put the removed servers in quarantine for a few seconds, they might come back.
        // Reassign the ranges from the removed servers to the current servers starting from the least loaded.
        // Also, assign any newly added base ranges.
        let mut ranges_to_assign: Vec<RangeInfo>;
        {
            let mut previously_unassigned = self.unassigned_base_ranges.lock().unwrap();
            ranges_to_assign = std::mem::take(previously_unassigned.as_mut());
        }

        for removed_server in removed_servers.clone() {
            if let Some(ranges) = assignee_to_range_info.remove(&removed_server.identity) {
                ranges_to_assign.extend(ranges);
            }
        }
        debug!("Ranges to assign: {:?}.", ranges_to_assign);

        for range in ranges_to_assign.into_iter() {
            let next_server = server_heap.pop().unwrap();
            debug!("Assigning range {:?} to server {:?}.", range, next_server);
            assignee_to_range_info
                .entry(next_server.0 .1.clone())
                .or_insert_with(Vec::new)
                .push(range);
            server_heap.push(Reverse((next_server.0 .0 + 1, next_server.0 .1.clone())));
        }
        {
            let mut range_assignments = self.range_assignments.lock().unwrap();
            range_assignments.clear();
            for (assignee, ranges) in assignee_to_range_info {
                for range in ranges {
                    range_assignments.push(RangeAssignment {
                        assignee: assignee.clone(),
                        range,
                    });
                }
            }
            debug!("Range assignments: {:?}.", range_assignments);
        }
        new_ready_servers
    }
}

impl AssignmentComputation for AssignmentComputationImpl {
<<<<<<< HEAD
    fn register_range_server(&self, host_info: HostInfo) -> Receiver<i64> {
        debug!("Registering range server: {:?}.", host_info);
        // Note that if this is a re-registration, the old receiver will
        // eventually be dropped when the gRPC stream for the old connection is
        // closed. gRPC will detect the disconnect when it tries to send an
        // update on the old connection.
        self.ready_range_servers
            .lock()
            .unwrap()
            .insert(HostInfoWrapper(host_info.clone()));
        self.assignment_update_sender.subscribe()
    }

    fn get_assignment_update(
        &self,
        host_info: &HostInfo,
        version: i64,
        full_update: bool,
    ) -> Option<WardenUpdate> {
=======
    fn register_range_server(&self, _host_info: HostInfo) -> Result<Receiver<i64>, Status> {
>>>>>>> origin/main
        todo!()
    }

    fn notify_range_server_unavailable(&self, host_info: HostInfo) {
        // TODO(purujit): Implement Quarantine.
        debug!("Notifying range server {:?} is unavailable.", host_info);
        self.ready_range_servers
            .lock()
            .unwrap()
            .remove(&HostInfoWrapper(host_info));
    }
}
#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use common::{
        key_range::KeyRange,
        region::{Region, Zone},
    };
    use once_cell::sync::Lazy;
    use uuid::Uuid;

    use super::*;
    use std::sync::Arc;
    fn make_zone() -> Zone {
        Zone {
            region: Region {
                cloud: None,
                name: "test".to_string(),
            },
            name: "test_zone".to_string(),
        }
    }

    fn make_range(start: u8, end: u8) -> RangeInfo {
        RangeInfo {
            key_range: KeyRange {
                lower_bound_inclusive: Some(Bytes::from(vec![start])),
                upper_bound_exclusive: Some(Bytes::from(vec![end])),
            },
            id: Uuid::new_v4(),
        }
    }

    static RUNTIME: Lazy<tokio::runtime::Runtime> =
        Lazy::new(|| tokio::runtime::Runtime::new().unwrap());
    fn setup() -> Arc<AssignmentComputationImpl> {
        let cancellation_token = CancellationToken::new();
        let computation =
            AssignmentComputationImpl::new(RUNTIME.handle().clone(), cancellation_token);
        computation
    }

    #[tokio::test]
    async fn test_run_assignment_computation_empty() {
        let computation = setup();
        let computation_clone = computation.clone();
        computation_clone
            .run_assignment_computation(HashSet::new())
            .await;

        let range_assignments = computation.range_assignments.lock().unwrap();
        assert!(range_assignments.is_empty());
    }

    #[tokio::test]
    async fn test_run_assignment_computation_single_server() {
        let computation = setup();
        let computation_clone = computation.clone();

        // Populate unassigned_base_ranges with one range and have one server to assign.
        let range = make_range(0, 127);
        computation
            .unassigned_base_ranges
            .lock()
            .unwrap()
            .push(range.clone());

        computation
            .ready_range_servers
            .lock()
            .unwrap()
            .insert(HostInfoWrapper(HostInfo {
                identity: "server1".to_string(),
                address: "1.2.3.4:8080".parse().unwrap(),
                zone: make_zone(),
            }));

        computation_clone
            .run_assignment_computation(HashSet::new())
            .await;

        let range_assignments = computation.range_assignments.lock().unwrap();
        assert_eq!(range_assignments.len(), 1);

        assert_eq!(range_assignments[0].assignee.to_string(), "server1");
        assert_eq!(range_assignments[0].range, range);
    }

    #[tokio::test]
    async fn test_run_assignment_computation_multiple_servers() {
        let computation = setup();
        let computation_clone = computation.clone();

        // Populate unassigned_base_ranges with 3 ranges and have 2 servers to assign.
        let ranges = vec![
            make_range(0, 127),
            make_range(128, 211),
            make_range(212, 255),
        ];
        let expected_ranges = ranges.clone();
        computation
            .unassigned_base_ranges
            .lock()
            .unwrap()
            .extend(ranges);
        let servers = vec![
            HostInfoWrapper(HostInfo {
                identity: "server1".to_string(),
                address: "1.2.3.4:8080".parse().unwrap(),
                zone: make_zone(),
            }),
            HostInfoWrapper(HostInfo {
                identity: "server2".to_string(),
                address: "5.6.7.8:8081".parse().unwrap(),
                zone: make_zone(),
            }),
        ];
        computation
            .ready_range_servers
            .lock()
            .unwrap()
            .extend(servers);

        computation_clone
            .run_assignment_computation(HashSet::new())
            .await;

        let range_assignments = computation.range_assignments.lock().unwrap();
        assert_eq!(range_assignments.len(), 3);

        let mut assigned_ranges: Vec<RangeInfo> =
            range_assignments.iter().map(|a| a.range.clone()).collect();
        let assigned_servers: HashSet<String> = range_assignments
            .iter()
            .map(|a| a.assignee.to_string())
            .collect();

        assigned_ranges.sort_by(|a, b| {
            a.key_range
                .lower_bound_inclusive
                .cmp(&b.key_range.lower_bound_inclusive)
        });
        assert_eq!(assigned_ranges, expected_ranges);

        assert_eq!(
            assigned_servers,
            HashSet::from_iter(vec!["server1".to_string(), "server2".to_string()])
        );
    }

    #[tokio::test]
    async fn test_run_assignment_computation_reassign_unavailable_server() {
        let computation = setup();
        let computation_clone = computation.clone();

        // Populate unassigned_base_ranges with 2 ranges and have 2 servers to assign.
        let ranges = vec![make_range(0, 127), make_range(128, 255)];

        let servers = vec![
            HostInfoWrapper(HostInfo {
                identity: "server1".to_string(),
                address: "1.2.3.4:8080".parse().unwrap(),
                zone: make_zone(),
            }),
            HostInfoWrapper(HostInfo {
                identity: "server2".to_string(),
                address: "5.6.7.8:8081".parse().unwrap(),
                zone: make_zone(),
            }),
        ];

        // Add a previous assignment for an unavailable server
        computation.range_assignments.lock().unwrap().extend(vec![
            RangeAssignment {
                range: ranges[0].clone(),
                assignee: "unavailable_server".to_string(),
            },
            RangeAssignment {
                range: ranges[1].clone(),
                assignee: "server1".to_string(),
            },
        ]);

        // Only add server2 to ready_range_servers
        computation
            .ready_range_servers
            .lock()
            .unwrap()
            .insert(servers[1].clone());

        let prev_servers: HashSet<HostInfoWrapper> = HashSet::from_iter(vec![
            HostInfoWrapper(HostInfo {
                identity: "unavailable_server".to_string(),
                address: "0.0.0.0:0".parse().unwrap(),
                zone: make_zone(),
            }),
            servers[0].clone(),
        ]);
        computation_clone
            .run_assignment_computation(prev_servers)
            .await;

        let range_assignments = computation.range_assignments.lock().unwrap();

        assert_eq!(range_assignments.len(), 2);
        let mut assigned_ranges: Vec<RangeInfo> =
            range_assignments.iter().map(|a| a.range.clone()).collect();
        let assigned_servers: HashSet<String> = range_assignments
            .iter()
            .map(|a| a.assignee.to_string())
            .collect();

        assigned_ranges.sort_by(|a, b| {
            a.key_range
                .lower_bound_inclusive
                .cmp(&b.key_range.lower_bound_inclusive)
        });
        assert_eq!(assigned_ranges, ranges);

        assert_eq!(
            assigned_servers,
            HashSet::from_iter(vec!["server1".to_string(), "server2".to_string()])
        );
    }

    #[tokio::test]
    async fn test_register_new_range_server() {
        let computation = setup();
        let server = HostInfo {
            identity: "new_server".to_string(),
            address: "127.0.0.1:8080".parse().unwrap(),
            zone: make_zone(),
        };

        computation.register_range_server(server.clone());

        let ready_servers = computation.ready_range_servers.lock().unwrap();
        assert!(ready_servers.contains(&HostInfoWrapper(server)));
    }

    #[tokio::test]
    async fn test_notify_range_server_unavailability() {
        let computation = setup();
        let server = HostInfo {
            identity: "server1".to_string(),
            address: "127.0.0.1:8080".parse().unwrap(),
            zone: make_zone(),
        };

        computation.register_range_server(server.clone());
        {
            let ready_servers = computation.ready_range_servers.lock().unwrap();
            assert!(ready_servers.contains(&HostInfoWrapper(server.clone())));
        }
        computation.notify_range_server_unavailable(server.clone());
        let ready_servers = computation.ready_range_servers.lock().unwrap();
        assert!(!ready_servers.contains(&HostInfoWrapper(server)));
    }
}
#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use common::{
        key_range::KeyRange,
        region::{Region, Zone},
    };
    use once_cell::sync::Lazy;
    use uuid::Uuid;

    use super::*;
    use std::sync::Arc;
    fn make_zone() -> Zone {
        Zone {
            region: Region {
                cloud: None,
                name: "test".to_string(),
            },
            name: "test_zone".to_string(),
        }
    }

    fn make_range(start: u8, end: u8) -> RangeInfo {
        RangeInfo {
            key_range: KeyRange {
                lower_bound_inclusive: Some(Bytes::from(vec![start])),
                upper_bound_exclusive: Some(Bytes::from(vec![end])),
            },
            id: Uuid::new_v4(),
        }
    }

    static RUNTIME: Lazy<tokio::runtime::Runtime> =
        Lazy::new(|| tokio::runtime::Runtime::new().unwrap());
    fn setup() -> Arc<AssignmentComputationImpl> {
        let cancellation_token = CancellationToken::new();
        let computation =
            AssignmentComputationImpl::new(RUNTIME.handle().clone(), cancellation_token);
        computation
    }

    #[tokio::test]
    async fn test_run_assignment_computation_empty() {
        let computation = setup();
        let computation_clone = computation.clone();
        computation_clone
            .run_assignment_computation(HashSet::new())
            .await;

        let range_assignments = computation.range_assignments.lock().unwrap();
        assert!(range_assignments.is_empty());
    }

    #[tokio::test]
    async fn test_run_assignment_computation_single_server() {
        let computation = setup();
        let computation_clone = computation.clone();

        // Populate unassigned_base_ranges with one range and have one server to assign.
        let range = make_range(0, 127);
        computation
            .unassigned_base_ranges
            .lock()
            .unwrap()
            .push(range.clone());

        computation
            .ready_range_servers
            .lock()
            .unwrap()
            .insert(HostInfoWrapper(HostInfo {
                identity: "server1".to_string(),
                address: "1.2.3.4:8080".parse().unwrap(),
                zone: make_zone(),
            }));

        computation_clone
            .run_assignment_computation(HashSet::new())
            .await;

        let range_assignments = computation.range_assignments.lock().unwrap();
        assert_eq!(range_assignments.len(), 1);

        assert_eq!(range_assignments[0].assignee.to_string(), "server1");
        assert_eq!(range_assignments[0].range, range);
    }

    #[tokio::test]
    async fn test_run_assignment_computation_multiple_servers() {
        let computation = setup();
        let computation_clone = computation.clone();

        // Populate unassigned_base_ranges with 3 ranges and have 2 servers to assign.
        let ranges = vec![
            make_range(0, 127),
            make_range(128, 211),
            make_range(212, 255),
        ];
        let expected_ranges = ranges.clone();
        computation
            .unassigned_base_ranges
            .lock()
            .unwrap()
            .extend(ranges);
        let servers = vec![
            HostInfoWrapper(HostInfo {
                identity: "server1".to_string(),
                address: "1.2.3.4:8080".parse().unwrap(),
                zone: make_zone(),
            }),
            HostInfoWrapper(HostInfo {
                identity: "server2".to_string(),
                address: "5.6.7.8:8081".parse().unwrap(),
                zone: make_zone(),
            }),
        ];
        computation
            .ready_range_servers
            .lock()
            .unwrap()
            .extend(servers);

        computation_clone
            .run_assignment_computation(HashSet::new())
            .await;

        let range_assignments = computation.range_assignments.lock().unwrap();
        assert_eq!(range_assignments.len(), 3);

        let mut assigned_ranges: Vec<RangeInfo> =
            range_assignments.iter().map(|a| a.range.clone()).collect();
        let assigned_servers: HashSet<String> = range_assignments
            .iter()
            .map(|a| a.assignee.to_string())
            .collect();

        assigned_ranges.sort_by(|a, b| {
            a.key_range
                .lower_bound_inclusive
                .cmp(&b.key_range.lower_bound_inclusive)
        });
        assert_eq!(assigned_ranges, expected_ranges);

        assert_eq!(
            assigned_servers,
            HashSet::from_iter(vec!["server1".to_string(), "server2".to_string()])
        );
    }

    #[tokio::test]
    async fn test_run_assignment_computation_reassign_unavailable_server() {
        let computation = setup();
        let computation_clone = computation.clone();

        // Populate unassigned_base_ranges with 2 ranges and have 2 servers to assign.
        let ranges = vec![make_range(0, 127), make_range(128, 255)];

        let servers = vec![
            HostInfoWrapper(HostInfo {
                identity: "server1".to_string(),
                address: "1.2.3.4:8080".parse().unwrap(),
                zone: make_zone(),
            }),
            HostInfoWrapper(HostInfo {
                identity: "server2".to_string(),
                address: "5.6.7.8:8081".parse().unwrap(),
                zone: make_zone(),
            }),
        ];

        // Add a previous assignment for an unavailable server
        computation.range_assignments.lock().unwrap().extend(vec![
            RangeAssignment {
                range: ranges[0].clone(),
                assignee: "unavailable_server".to_string(),
            },
            RangeAssignment {
                range: ranges[1].clone(),
                assignee: "server1".to_string(),
            },
        ]);

        // Only add server2 to ready_range_servers
        computation
            .ready_range_servers
            .lock()
            .unwrap()
            .insert(servers[1].clone());

        let prev_servers: HashSet<HostInfoWrapper> = HashSet::from_iter(vec![
            HostInfoWrapper(HostInfo {
                identity: "unavailable_server".to_string(),
                address: "0.0.0.0:0".parse().unwrap(),
                zone: make_zone(),
            }),
            servers[0].clone(),
        ]);
        computation_clone
            .run_assignment_computation(prev_servers)
            .await;

        let range_assignments = computation.range_assignments.lock().unwrap();

        assert_eq!(range_assignments.len(), 2);
        let mut assigned_ranges: Vec<RangeInfo> =
            range_assignments.iter().map(|a| a.range.clone()).collect();
        let assigned_servers: HashSet<String> = range_assignments
            .iter()
            .map(|a| a.assignee.to_string())
            .collect();

        assigned_ranges.sort_by(|a, b| {
            a.key_range
                .lower_bound_inclusive
                .cmp(&b.key_range.lower_bound_inclusive)
        });
        assert_eq!(assigned_ranges, ranges);

        assert_eq!(
            assigned_servers,
            HashSet::from_iter(vec!["server1".to_string(), "server2".to_string()])
        );
    }
}
