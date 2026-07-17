use crate::manager::domain;
use crate::manager::grpc::common::v1;
use crate::manager::grpc::common::v1::Partition;
use std::collections::HashMap;

pub(super) fn grpc_node_type_to_domain(node_type: i32) -> domain::NodeType {
    match v1::NodeType::try_from(node_type).ok() {
        Some(v1::NodeType::Worker) => domain::NodeType::Worker,
        _ => domain::NodeType::Manager,
    }
}

pub(super) fn domain_node_type_to_grpc(node_type: domain::NodeType) -> i32 {
    match node_type {
        domain::NodeType::Manager => v1::NodeType::Manager as i32,
        domain::NodeType::Worker => v1::NodeType::Worker as i32,
    }
}

pub(super) fn grpc_partitions_to_domain(
    partitions: HashMap<u32, Partition>,
) -> HashMap<u16, domain::Partition> {
    grpc_partition_mapping_to_domain(partitions)
}

pub(super) fn domain_partitions_to_grpc(
    partitions: HashMap<u16, domain::Partition>,
) -> HashMap<u32, Partition> {
    domain_partition_mapping_to_grpc(partitions)
}

fn grpc_partition_mapping_to_domain(
    mapping: HashMap<u32, Partition>,
) -> HashMap<u16, domain::Partition> {
    mapping
        .into_iter()
        .map(|(partition_id, partition)| {
            (
                partition_id as u16,
                domain::Partition {
                    master: partition.master.into(),
                    replicas: partition
                        .replicas
                        .into_iter()
                        .map(|replica| replica.into())
                        .collect(),
                },
            )
        })
        .collect()
}

fn domain_partition_mapping_to_grpc(
    mapping: HashMap<u16, domain::Partition>,
) -> HashMap<u32, Partition> {
    mapping
        .into_iter()
        .map(|(partition_id, partition)| {
            (
                partition_id as u32,
                Partition {
                    master: partition.master.to_string(),
                    replicas: partition
                        .replicas
                        .into_iter()
                        .map(|node| node.to_string())
                        .collect(),
                },
            )
        })
        .collect()
}
