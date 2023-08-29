//! `ComputeSpec` represents the contents of the spec.json file.
//!
//! The spec.json file is used to pass information to 'compute_ctl'. It contains
//! all the information needed to start up the right version of PostgreSQL,
//! and connect it to the storage nodes.
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use utils::id::{TenantId, TimelineId};
use utils::lsn::Lsn;

/// String type alias representing Postgres identifier and
/// intended to be used for DB / role names.
pub type PgIdent = String;

/// Cluster spec or configuration represented as an optional number of
/// delta operations + final cluster state description.
#[serde_as]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ComputeSpec {
    pub format_version: f32,

    // The control plane also includes a 'timestamp' field in the JSON document,
    // but we don't use it for anything. Serde will ignore missing fields when
    // deserializing it.
    pub operation_uuid: Option<String>,
    /// Expected cluster state at the end of transition process.
    pub cluster: Cluster,
    pub delta_operations: Option<Vec<DeltaOp>>,

    /// An optinal hint that can be passed to speed up startup time if we know
    /// that no pg catalog mutations (like role creation, database creation,
    /// extension creation) need to be done on the actual database to start.
    #[serde(default)] // Default false
    pub skip_pg_catalog_updates: bool,

    // Information needed to connect to the storage layer.
    //
    // `tenant_id`, `timeline_id` and `pageserver_connstring` are always needed.
    //
    // Depending on `mode`, this can be a primary read-write node, a read-only
    // replica, or a read-only node pinned at an older LSN.
    // `safekeeper_connstrings` must be set for a primary.
    //
    // For backwards compatibility, the control plane may leave out all of
    // these, and instead set the "neon.tenant_id", "neon.timeline_id",
    // etc. GUCs in cluster.settings. TODO: Once the control plane has been
    // updated to fill these fields, we can make these non optional.
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub tenant_id: Option<TenantId>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub timeline_id: Option<TimelineId>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub pageserver_connstring: Option<String>,
    #[serde(default)]
    pub safekeeper_connstrings: Vec<String>,

    #[serde(default)]
    pub mode: ComputeMode,

    /// If set, 'storage_auth_token' is used as the password to authenticate to
    /// the pageserver and safekeepers.
    pub storage_auth_token: Option<String>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
pub enum ComputeMode {
    /// A read-write node
    #[default]
    Primary,
    /// A read-only node, pinned at a particular LSN
    Static(#[serde_as(as = "DisplayFromStr")] Lsn),
    /// A read-only node that follows the tip of the branch in hot standby mode
    ///
    /// Future versions may want to distinguish between replicas with hot standby
    /// feedback and other kinds of replication configurations.
    Replica,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Cluster {
    pub cluster_id: Option<String>,
    pub name: Option<String>,
    pub state: Option<String>,
    pub roles: Vec<Role>,
    pub databases: Vec<Database>,

    /// Desired contents of 'postgresql.conf' file. (The 'compute_ctl'
    /// tool may add additional settings to the final file.)
    pub postgresql_conf: Option<String>,

    /// Additional settings that will be appended to the 'postgresql.conf' file.
    pub settings: GenericOptions,
}

/// Single cluster state changing operation that could not be represented as
/// a static `Cluster` structure. For example:
/// - DROP DATABASE
/// - DROP ROLE
/// - ALTER ROLE name RENAME TO new_name
/// - ALTER DATABASE name RENAME TO new_name
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DeltaOp {
    pub action: String,
    pub name: PgIdent,
    pub new_name: Option<PgIdent>,
}

/// Rust representation of Postgres role info with only those fields
/// that matter for us.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Role {
    pub name: PgIdent,
    pub encrypted_password: Option<String>,
    pub options: GenericOptions,
}

/// Rust representation of Postgres database info with only those fields
/// that matter for us.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Database {
    pub name: PgIdent,
    pub owner: PgIdent,
    pub options: GenericOptions,
}

/// Common type representing both SQL statement params with or without value,
/// like `LOGIN` or `OWNER username` in the `CREATE/ALTER ROLE`, and config
/// options like `wal_level = logical`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GenericOption {
    pub name: String,
    pub value: Option<String>,
    pub vartype: String,
}

/// Optional collection of `GenericOption`'s. Type alias allows us to
/// declare a `trait` on it.
pub type GenericOptions = Option<Vec<GenericOption>>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    #[test]
    fn parse_spec_file() {
        let file = File::open("tests/cluster_spec.json").unwrap();
        let _spec: ComputeSpec = serde_json::from_reader(file).unwrap();
    }

    #[test]
    fn parse_unknown_fields() {
        // Forward compatibility test
        let file = File::open("tests/cluster_spec.json").unwrap();
        let mut json: serde_json::Value = serde_json::from_reader(file).unwrap();
        let ob = json.as_object_mut().unwrap();
        ob.insert("unknown_field_123123123".into(), "hello".into());
        let _spec: ComputeSpec = serde_json::from_value(json).unwrap();
    }
}
