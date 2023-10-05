// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! In-memory metadata storage for the coordinator.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::net::Ipv4Addr;
use std::time::Instant;

use anyhow::bail;
use itertools::Itertools;
use serde::Serialize;
use tracing::info;

use mz_audit_log::{EventDetails, EventType, ObjectType, VersionedEvent, VersionedStorageUsage};
use mz_build_info::DUMMY_BUILD_INFO;
use mz_catalog::builtin::{Builtin, BuiltinCluster, BuiltinLog, BuiltinSource, BuiltinTable};
use mz_controller::clusters::{
    ClusterStatus, ManagedReplicaLocation, ProcessId, ReplicaConfig, ReplicaLocation,
};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_expr::MirScalarExpr;
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::now::{to_datetime, EpochMillis, NOW_ZERO};
use mz_ore::soft_assert;
use mz_repr::adt::mz_acl_item::PrivilegeMap;
use mz_repr::namespaces::{
    INFORMATION_SCHEMA, MZ_CATALOG_SCHEMA, MZ_INTERNAL_SCHEMA, MZ_TEMP_SCHEMA, PG_CATALOG_SCHEMA,
};
use mz_repr::role_id::RoleId;
use mz_repr::{GlobalId, RelationDesc};
use mz_sql::catalog::{
    CatalogCluster, CatalogClusterReplica, CatalogConfig, CatalogDatabase,
    CatalogError as SqlCatalogError, CatalogItem as SqlCatalogItem, CatalogItemType, CatalogRole,
    CatalogSchema, EnvironmentId, IdReference, SessionCatalog, SystemObjectType, TypeReference,
};
use mz_sql::names::{
    CommentObjectId, DatabaseId, FullItemName, FullSchemaName, ItemQualifiers, ObjectId,
    PartialItemName, QualifiedItemName, QualifiedSchemaName, RawDatabaseSpecifier,
    ResolvedDatabaseSpecifier, ResolvedIds, SchemaId, SchemaSpecifier, SystemObjectId,
};
use mz_sql::plan::{CreateViewPlan, Params, Plan};
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
use mz_sql::session::vars::{SystemVars, Var, VarInput};
use mz_sql_parser::ast::QualifiedReplica;
use mz_storage_types::connections::inline::{
    ConnectionResolver, InlinedConnection, IntoInlineConnection,
};
use mz_transform::Optimizer;

use crate::catalog::{
    AwsPrincipalContext, BuiltinTableUpdate, Catalog, CatalogEntry, CatalogItem, Cluster,
    ClusterConfig, ClusterReplica, ClusterReplicaProcessStatus, ClusterReplicaSizeMap, CommentsMap,
    Database, DefaultPrivileges, Error, ErrorKind, Index, Role, Schema, View,
    LINKED_CLUSTER_REPLICA_NAME, SYSTEM_CONN_ID,
};
use crate::client::ConnectionId;
use crate::coord::ConnMeta;
use crate::session::Session;
use crate::util::{index_sql, ResultExt};
use crate::AdapterError;

/// The in-memory representation of the Catalog. This struct is not directly used to persist
/// metadata to persistent storage. For persistent metadata see [`mz_catalog::DurableCatalogState`].
///
/// [`Serialize`] is implemented to create human readable dumps of the in-memory state, not for
/// storing the contents of this struct on disk.
#[derive(Debug, Clone, Serialize)]
pub struct CatalogState {
    pub(super) database_by_name: BTreeMap<String, DatabaseId>,
    #[serde(serialize_with = "mz_ore::serde::map_key_to_string")]
    pub(super) database_by_id: BTreeMap<DatabaseId, Database>,
    #[serde(serialize_with = "skip_temp_items")]
    pub(super) entry_by_id: BTreeMap<GlobalId, CatalogEntry>,
    pub(super) ambient_schemas_by_name: BTreeMap<String, SchemaId>,
    #[serde(serialize_with = "mz_ore::serde::map_key_to_string")]
    pub(super) ambient_schemas_by_id: BTreeMap<SchemaId, Schema>,
    #[serde(skip)]
    pub(super) temporary_schemas: BTreeMap<ConnectionId, Schema>,
    #[serde(serialize_with = "mz_ore::serde::map_key_to_string")]
    pub(super) clusters_by_id: BTreeMap<ClusterId, Cluster>,
    pub(super) clusters_by_name: BTreeMap<String, ClusterId>,
    #[serde(serialize_with = "mz_ore::serde::map_key_to_string")]
    pub(super) clusters_by_linked_object_id: BTreeMap<GlobalId, ClusterId>,
    pub(super) roles_by_name: BTreeMap<String, RoleId>,
    #[serde(serialize_with = "mz_ore::serde::map_key_to_string")]
    pub(super) roles_by_id: BTreeMap<RoleId, Role>,
    #[serde(skip)]
    pub(super) config: mz_sql::catalog::CatalogConfig,
    #[serde(skip)]
    pub(super) oid_counter: u32,
    pub(super) cluster_replica_sizes: ClusterReplicaSizeMap,
    #[serde(skip)]
    pub(super) default_storage_cluster_size: Option<String>,
    #[serde(skip)]
    pub(crate) availability_zones: Vec<String>,
    #[serde(skip)]
    pub(super) system_configuration: SystemVars,
    pub(super) egress_ips: Vec<Ipv4Addr>,
    pub(super) aws_principal_context: Option<AwsPrincipalContext>,
    pub(super) aws_privatelink_availability_zones: Option<BTreeSet<String>>,
    pub(super) http_host_name: Option<String>,
    pub(super) default_privileges: DefaultPrivileges,
    pub(super) system_privileges: PrivilegeMap,
    pub(super) comments: CommentsMap,
}

fn skip_temp_items<S>(
    entries: &BTreeMap<GlobalId, CatalogEntry>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    mz_ore::serde::map_key_to_string(
        entries.iter().filter(|(_k, v)| v.conn_id().is_none()),
        serializer,
    )
}

impl CatalogState {
    pub fn empty() -> Self {
        CatalogState {
            database_by_name: Default::default(),
            database_by_id: Default::default(),
            entry_by_id: Default::default(),
            ambient_schemas_by_name: Default::default(),
            ambient_schemas_by_id: Default::default(),
            temporary_schemas: Default::default(),
            clusters_by_id: Default::default(),
            clusters_by_name: Default::default(),
            clusters_by_linked_object_id: Default::default(),
            roles_by_name: Default::default(),
            roles_by_id: Default::default(),
            config: CatalogConfig {
                start_time: Default::default(),
                start_instant: Instant::now(),
                nonce: Default::default(),
                environment_id: EnvironmentId::for_tests(),
                session_id: Default::default(),
                build_info: &DUMMY_BUILD_INFO,
                timestamp_interval: Default::default(),
                now: NOW_ZERO.clone(),
            },
            oid_counter: Default::default(),
            cluster_replica_sizes: Default::default(),
            default_storage_cluster_size: Default::default(),
            availability_zones: Default::default(),
            system_configuration: Default::default(),
            egress_ips: Default::default(),
            aws_principal_context: Default::default(),
            aws_privatelink_availability_zones: Default::default(),
            http_host_name: Default::default(),
            default_privileges: Default::default(),
            system_privileges: Default::default(),
            comments: Default::default(),
        }
    }

    pub fn allocate_oid(&mut self) -> Result<u32, Error> {
        let oid = self.oid_counter;
        if oid == u32::max_value() {
            return Err(Error::new(ErrorKind::OidExhaustion));
        }
        self.oid_counter += 1;
        Ok(oid)
    }

    /// Computes the IDs of any indexes that transitively depend on this catalog
    /// entry.
    pub fn dependent_indexes(&self, id: GlobalId) -> Vec<GlobalId> {
        let mut out = Vec::new();
        self.dependent_indexes_inner(id, &mut out);
        out
    }

    fn dependent_indexes_inner(&self, id: GlobalId, out: &mut Vec<GlobalId>) {
        let entry = self.get_entry(&id);
        match entry.item() {
            CatalogItem::Index(_) => out.push(id),
            _ => {
                for id in entry.used_by() {
                    self.dependent_indexes_inner(*id, out)
                }
            }
        }
    }

    /// Computes the IDs of any log sources this catalog entry transitively
    /// depends on.
    pub fn introspection_dependencies(&self, id: GlobalId) -> Vec<GlobalId> {
        let mut out = Vec::new();
        self.introspection_dependencies_inner(id, &mut out);
        out
    }

    fn introspection_dependencies_inner(&self, id: GlobalId, out: &mut Vec<GlobalId>) {
        match self.get_entry(&id).item() {
            CatalogItem::Log(_) => out.push(id),
            item @ (CatalogItem::View(_)
            | CatalogItem::MaterializedView(_)
            | CatalogItem::Connection(_)) => {
                for id in &item.uses().0 {
                    self.introspection_dependencies_inner(*id, out);
                }
            }
            CatalogItem::Sink(sink) => self.introspection_dependencies_inner(sink.from, out),
            CatalogItem::Index(idx) => self.introspection_dependencies_inner(idx.on, out),
            CatalogItem::Table(_)
            | CatalogItem::Source(_)
            | CatalogItem::Type(_)
            | CatalogItem::Func(_)
            | CatalogItem::Secret(_) => (),
        }
    }

    /// Returns all the IDs of all objects that depend on `ids`, including `ids` themselves.
    ///
    /// The order is guaranteed to be in reverse dependency order, i.e. the leafs will appear
    /// earlier in the list than the roots. This is particularly userful for the order to drop
    /// objects.
    pub(super) fn object_dependents(
        &self,
        object_ids: &Vec<ObjectId>,
        conn_id: &ConnectionId,
        seen: &mut BTreeSet<ObjectId>,
    ) -> Vec<ObjectId> {
        let mut dependents = Vec::new();
        for object_id in object_ids {
            match object_id {
                ObjectId::Cluster(id) => {
                    dependents.extend_from_slice(&self.cluster_dependents(*id, seen));
                }
                ObjectId::ClusterReplica((cluster_id, replica_id)) => dependents.extend_from_slice(
                    &self.cluster_replica_dependents(*cluster_id, *replica_id, seen),
                ),
                ObjectId::Database(id) => {
                    dependents.extend_from_slice(&self.database_dependents(*id, conn_id, seen))
                }
                ObjectId::Schema((database_spec, schema_spec)) => {
                    dependents.extend_from_slice(&self.schema_dependents(
                        database_spec.clone(),
                        schema_spec.clone(),
                        conn_id,
                        seen,
                    ));
                }
                id @ ObjectId::Role(_) => {
                    let unseen = seen.insert(id.clone());
                    if unseen {
                        dependents.push(id.clone());
                    }
                }
                ObjectId::Item(id) => {
                    dependents.extend_from_slice(&self.item_dependents(*id, seen))
                }
            }
        }
        dependents
    }

    /// Returns all the IDs of all objects that depend on `cluster_id`, including `cluster_id`
    /// itself.
    ///
    /// The order is guaranteed to be in reverse dependency order, i.e. the leafs will appear
    /// earlier in the list than the roots. This is particularly userful for the order to drop
    /// objects.
    fn cluster_dependents(
        &self,
        cluster_id: ClusterId,
        seen: &mut BTreeSet<ObjectId>,
    ) -> Vec<ObjectId> {
        let mut dependents = Vec::new();
        let object_id = ObjectId::Cluster(cluster_id);
        if !seen.contains(&object_id) {
            seen.insert(object_id.clone());
            let cluster = self.get_cluster(cluster_id);
            for item_id in cluster.bound_objects() {
                dependents.extend_from_slice(&self.item_dependents(*item_id, seen));
            }
            for replica_id in cluster.replica_ids().values() {
                dependents.extend_from_slice(&self.cluster_replica_dependents(
                    cluster_id,
                    *replica_id,
                    seen,
                ));
            }
            dependents.push(object_id);
        }
        dependents
    }

    /// Returns all the IDs of all objects that depend on `replica_id`, including `replica_id`
    /// itself.
    ///
    /// The order is guaranteed to be in reverse dependency order, i.e. the leafs will appear
    /// earlier in the list than the roots. This is particularly userful for the order to drop
    /// objects.
    pub(super) fn cluster_replica_dependents(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        seen: &mut BTreeSet<ObjectId>,
    ) -> Vec<ObjectId> {
        let mut dependents = Vec::new();
        let object_id = ObjectId::ClusterReplica((cluster_id, replica_id));
        if !seen.contains(&object_id) {
            seen.insert(object_id.clone());
            dependents.push(object_id);
        }
        dependents
    }

    /// Returns all the IDs of all objects that depend on `database_id`, including `database_id`
    /// itself.
    ///
    /// The order is guaranteed to be in reverse dependency order, i.e. the leafs will appear
    /// earlier in the list than the roots. This is particularly userful for the order to drop
    /// objects.
    fn database_dependents(
        &self,
        database_id: DatabaseId,
        conn_id: &ConnectionId,
        seen: &mut BTreeSet<ObjectId>,
    ) -> Vec<ObjectId> {
        let mut dependents = Vec::new();
        let object_id = ObjectId::Database(database_id);
        if !seen.contains(&object_id) {
            seen.insert(object_id.clone());
            let database = self.get_database(&database_id);
            for schema_id in database.schema_ids().values() {
                dependents.extend_from_slice(&self.schema_dependents(
                    ResolvedDatabaseSpecifier::Id(database_id),
                    SchemaSpecifier::Id(*schema_id),
                    conn_id,
                    seen,
                ));
            }
            dependents.push(object_id);
        }
        dependents
    }

    /// Returns all the IDs of all objects that depend on `schema_id`, including `schema_id`
    /// itself.
    ///
    /// The order is guaranteed to be in reverse dependency order, i.e. the leafs will appear
    /// earlier in the list than the roots. This is particularly userful for the order to drop
    /// objects.
    fn schema_dependents(
        &self,
        database_spec: ResolvedDatabaseSpecifier,
        schema_spec: SchemaSpecifier,
        conn_id: &ConnectionId,
        seen: &mut BTreeSet<ObjectId>,
    ) -> Vec<ObjectId> {
        let mut dependents = Vec::new();
        let object_id = ObjectId::Schema((database_spec, schema_spec.clone()));
        if !seen.contains(&object_id) {
            seen.insert(object_id.clone());
            let schema = self.get_schema(&database_spec, &schema_spec, conn_id);
            for item_id in schema.item_ids().values() {
                dependents.extend_from_slice(&self.item_dependents(*item_id, seen));
            }
            dependents.push(object_id)
        }
        dependents
    }

    /// Returns all the IDs of all objects that depend on `item_id`, including `item_id`
    /// itself.
    ///
    /// The order is guaranteed to be in reverse dependency order, i.e. the leafs will appear
    /// earlier in the list than the roots. This is particularly userful for the order to drop
    /// objects.
    pub(super) fn item_dependents(
        &self,
        item_id: GlobalId,
        seen: &mut BTreeSet<ObjectId>,
    ) -> Vec<ObjectId> {
        let mut dependents = Vec::new();
        let object_id = ObjectId::Item(item_id);
        if !seen.contains(&object_id) {
            seen.insert(object_id.clone());
            for dependent_id in self.get_entry(&item_id).used_by() {
                dependents.extend_from_slice(&self.item_dependents(*dependent_id, seen));
            }
            for subsource_id in self.get_entry(&item_id).subsources() {
                dependents.extend_from_slice(&self.item_dependents(subsource_id, seen));
            }
            dependents.push(object_id);
            if let Some(linked_cluster_id) = self.clusters_by_linked_object_id.get(&item_id) {
                dependents.extend_from_slice(&self.cluster_dependents(*linked_cluster_id, seen));
            }
        }
        dependents
    }

    pub fn uses_tables(&self, id: GlobalId) -> bool {
        match self.get_entry(&id).item() {
            CatalogItem::Table(_) => true,
            item @ (CatalogItem::View(_) | CatalogItem::MaterializedView(_)) => {
                item.uses().0.iter().any(|id| self.uses_tables(*id))
            }
            CatalogItem::Index(idx) => self.uses_tables(idx.on),
            CatalogItem::Source(_)
            | CatalogItem::Log(_)
            | CatalogItem::Func(_)
            | CatalogItem::Sink(_)
            | CatalogItem::Type(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_) => false,
        }
    }

    /// Indicates whether the indicated item is considered stable or not.
    ///
    /// Only stable items can be used as dependencies of other catalog items.
    fn is_stable(&self, id: GlobalId) -> bool {
        let mz_internal_id = self.ambient_schemas_by_name[MZ_INTERNAL_SCHEMA];
        match &self.get_entry(&id).name().qualifiers.schema_spec {
            SchemaSpecifier::Temporary => true,
            SchemaSpecifier::Id(id) => *id != mz_internal_id,
        }
    }

    pub(super) fn check_unstable_dependencies(
        &self,
        item: &CatalogItem,
    ) -> Result<(), AdapterError> {
        if self.system_config().enable_unstable_dependencies() {
            return Ok(());
        }

        let unstable_dependencies: Vec<_> = item
            .uses()
            .0
            .iter()
            .filter(|id| !self.is_stable(**id))
            .map(|id| self.get_entry(id).name().item.clone())
            .collect();

        // It's okay to create a temporary object with unstable
        // dependencies, since we will never need to reboot a catalog
        // that contains it.
        if unstable_dependencies.is_empty() || item.is_temporary() {
            Ok(())
        } else {
            let object_type = item.typ().to_string();
            Err(AdapterError::UnstableDependency {
                object_type,
                unstable_dependencies,
            })
        }
    }

    pub fn resolve_full_name(
        &self,
        name: &QualifiedItemName,
        conn_id: Option<&ConnectionId>,
    ) -> FullItemName {
        let conn_id = conn_id.unwrap_or(&SYSTEM_CONN_ID);

        let database = match &name.qualifiers.database_spec {
            ResolvedDatabaseSpecifier::Ambient => RawDatabaseSpecifier::Ambient,
            ResolvedDatabaseSpecifier::Id(id) => {
                RawDatabaseSpecifier::Name(self.get_database(id).name().to_string())
            }
        };
        let schema = self
            .get_schema(
                &name.qualifiers.database_spec,
                &name.qualifiers.schema_spec,
                conn_id,
            )
            .name()
            .schema
            .clone();
        FullItemName {
            database,
            schema,
            item: name.item.clone(),
        }
    }

    pub(super) fn resolve_full_schema_name(&self, name: &QualifiedSchemaName) -> FullSchemaName {
        let database = match &name.database {
            ResolvedDatabaseSpecifier::Ambient => RawDatabaseSpecifier::Ambient,
            ResolvedDatabaseSpecifier::Id(id) => {
                RawDatabaseSpecifier::Name(self.get_database(id).name().to_string())
            }
        };
        FullSchemaName {
            database,
            schema: name.schema.clone(),
        }
    }

    pub fn get_entry(&self, id: &GlobalId) -> &CatalogEntry {
        &self.entry_by_id[id]
    }

    pub fn get_entry_mut(&mut self, id: &GlobalId) -> &mut CatalogEntry {
        self.entry_by_id.get_mut(id).expect("catalog out of sync")
    }

    pub fn try_get_entry_in_schema(
        &self,
        name: &QualifiedItemName,
        conn_id: &ConnectionId,
    ) -> Option<&CatalogEntry> {
        self.get_schema(
            &name.qualifiers.database_spec,
            &name.qualifiers.schema_spec,
            conn_id,
        )
        .items
        .get(&name.item)
        .and_then(|id| self.try_get_entry(id))
    }

    /// Gets an entry named `item` from exactly one of system schemas.
    ///
    /// # Panics
    /// - If `item` is not an entry in any system schema
    /// - If more than one system schema has an entry named `item`.
    pub(super) fn get_entry_in_system_schemas(&self, item: &str) -> &CatalogEntry {
        let mut res = None;
        for system_schema in &[
            PG_CATALOG_SCHEMA,
            INFORMATION_SCHEMA,
            MZ_CATALOG_SCHEMA,
            MZ_INTERNAL_SCHEMA,
        ] {
            let schema_id = &self.ambient_schemas_by_name[*system_schema];
            let schema = &self.ambient_schemas_by_id[schema_id];
            if let Some(global_id) = schema.items.get(item) {
                match res {
                    None => res = Some(self.get_entry(global_id)),
                    Some(_) => panic!("only call get_entry_in_system_schemas on objects uniquely identifiable in one system schema"),
                }
            }
        }

        res.unwrap_or_else(|| panic!("cannot find {} in system schema", item))
    }

    pub fn item_exists(&self, name: &QualifiedItemName, conn_id: &ConnectionId) -> bool {
        self.try_get_entry_in_schema(name, conn_id).is_some()
    }

    pub(super) fn find_available_name(
        &self,
        mut name: QualifiedItemName,
        conn_id: &ConnectionId,
    ) -> QualifiedItemName {
        let mut i = 0;
        let orig_item_name = name.item.clone();
        while self.item_exists(&name, conn_id) {
            i += 1;
            name.item = format!("{}{}", orig_item_name, i);
        }
        name
    }

    pub fn try_get_entry(&self, id: &GlobalId) -> Option<&CatalogEntry> {
        self.entry_by_id.get(id)
    }

    pub(crate) fn get_cluster(&self, cluster_id: ClusterId) -> &Cluster {
        self.try_get_cluster(cluster_id)
            .unwrap_or_else(|| panic!("unknown cluster {cluster_id}"))
    }

    pub(super) fn get_cluster_mut(&mut self, cluster_id: ClusterId) -> &mut Cluster {
        self.try_get_cluster_mut(cluster_id)
            .unwrap_or_else(|| panic!("unknown cluster {cluster_id}"))
    }

    pub(super) fn try_get_cluster(&self, cluster_id: ClusterId) -> Option<&Cluster> {
        self.clusters_by_id.get(&cluster_id)
    }

    pub(super) fn try_get_cluster_mut(&mut self, cluster_id: ClusterId) -> Option<&mut Cluster> {
        self.clusters_by_id.get_mut(&cluster_id)
    }

    pub(super) fn get_linked_cluster(&self, object_id: GlobalId) -> Option<&Cluster> {
        self.clusters_by_linked_object_id
            .get(&object_id)
            .map(|id| &self.clusters_by_id[id])
    }

    pub(super) fn get_storage_object_size(&self, object_id: GlobalId) -> Option<&str> {
        let cluster = self.get_linked_cluster(object_id)?;
        let replica_id = cluster
            .replica_id(LINKED_CLUSTER_REPLICA_NAME)
            .expect("Must exist");
        let replica = cluster.replica(replica_id).expect("Must exist");
        match &replica.config.location {
            ReplicaLocation::Unmanaged(_) => None,
            ReplicaLocation::Managed(ManagedReplicaLocation { size, .. }) => Some(size),
        }
    }

    pub(super) fn try_get_role(&self, id: &RoleId) -> Option<&Role> {
        self.roles_by_id.get(id)
    }

    pub fn get_role(&self, id: &RoleId) -> &Role {
        self.roles_by_id.get(id).expect("catalog out of sync")
    }

    pub fn get_roles(&self) -> impl Iterator<Item = &RoleId> {
        self.roles_by_id.keys()
    }

    pub(super) fn try_get_role_by_name(&self, role_name: &str) -> Option<&Role> {
        self.roles_by_name
            .get(role_name)
            .map(|id| &self.roles_by_id[id])
    }

    pub(super) fn get_role_mut(&mut self, id: &RoleId) -> &mut Role {
        self.roles_by_id.get_mut(id).expect("catalog out of sync")
    }

    pub(crate) fn collect_role_membership(&self, id: &RoleId) -> BTreeSet<RoleId> {
        let mut membership = BTreeSet::new();
        let mut queue = VecDeque::from(vec![id]);
        while let Some(cur_id) = queue.pop_front() {
            if !membership.contains(cur_id) {
                membership.insert(cur_id.clone());
                let role = self.get_role(cur_id);
                soft_assert!(
                    !role.membership().keys().contains(id),
                    "circular membership exists in the catalog"
                );
                queue.extend(role.membership().keys());
            }
        }
        membership.insert(RoleId::Public);
        membership
    }

    /// Returns the URL for POST-ing data to a webhook source, if `id` corresponds to a webhook
    /// source.
    ///
    /// Note: Identifiers for the source, e.g. item name, are URL encoded.
    pub fn try_get_webhook_url(&self, id: &GlobalId) -> Option<url::Url> {
        let entry = self.try_get_entry(id)?;
        // Note: Webhook sources can never be created in the temporary schema, hence passing None.
        let name = self.resolve_full_name(entry.name(), None);
        let host_name = self
            .http_host_name
            .as_ref()
            .map(|x| x.as_str())
            .unwrap_or_else(|| "HOST");

        let RawDatabaseSpecifier::Name(database) = name.database else {
            return None;
        };

        let mut url = url::Url::parse(&format!("https://{host_name}/api/webhook")).ok()?;
        url.path_segments_mut()
            .ok()?
            .push(&database)
            .push(&name.schema)
            .push(&name.item);

        Some(url)
    }

    /// Parse a SQL string into a catalog view item with only a limited
    /// context.
    #[tracing::instrument(level = "info", skip_all)]
    pub fn parse_view_item(&self, create_sql: String) -> Result<CatalogItem, anyhow::Error> {
        let mut session_catalog = Catalog::for_system_session_state(self);

        // Enable catalog features that might be required during planning in
        // [Catalog::open]. Existing catalog items might have been created while a
        // specific feature flag turned on, so we need to ensure that this is also the
        // case during catalog rehydration in order to avoid panics.
        session_catalog.system_vars_mut().enable_all_feature_flags();

        let stmt = mz_sql::parse::parse(&create_sql)?.into_element().ast;
        let (stmt, resolved_ids) = mz_sql::names::resolve(&session_catalog, stmt)?;
        let plan = mz_sql::plan::plan(
            None,
            &session_catalog,
            stmt,
            &Params::empty(),
            &resolved_ids,
        )?;
        Ok(match plan {
            Plan::CreateView(CreateViewPlan { view, .. }) => {
                let optimizer =
                    Optimizer::logical_optimizer(&mz_transform::typecheck::empty_context());
                let raw_expr = view.expr;
                let decorrelated_expr =
                    raw_expr.optimize_and_lower(&mz_sql::plan::OptimizerConfig {})?;
                let optimized_expr = optimizer.optimize(decorrelated_expr)?;
                let desc = RelationDesc::new(optimized_expr.typ(), view.column_names);
                CatalogItem::View(View {
                    create_sql: view.create_sql,
                    optimized_expr,
                    desc,
                    conn_id: None,
                    resolved_ids,
                })
            }
            _ => bail!("Expected valid CREATE VIEW statement"),
        })
    }

    /// Returns all indexes on the given object and cluster known in the
    /// catalog.
    pub fn get_indexes_on(
        &self,
        id: GlobalId,
        cluster: ClusterId,
    ) -> impl Iterator<Item = (GlobalId, &Index)> {
        let index_matches = move |idx: &Index| idx.on == id && idx.cluster_id == cluster;

        self.try_get_entry(&id)
            .into_iter()
            .map(move |e| {
                e.used_by()
                    .iter()
                    .filter_map(move |uses_id| match self.get_entry(uses_id).item() {
                        CatalogItem::Index(index) if index_matches(index) => {
                            Some((*uses_id, index))
                        }
                        _ => None,
                    })
            })
            .flatten()
    }

    /// Associates a name, `GlobalId`, and entry.
    pub(super) fn insert_item(
        &mut self,
        id: GlobalId,
        oid: u32,
        name: QualifiedItemName,
        item: CatalogItem,
        owner_id: RoleId,
        privileges: PrivilegeMap,
    ) {
        if !id.is_system() && !item.is_placeholder() {
            info!(
                "create {} {} ({})",
                item.typ(),
                self.resolve_full_name(&name, None),
                id
            );
        }

        if !id.is_system() {
            if let Some(cluster_id) = item.cluster_id() {
                self.clusters_by_id
                    .get_mut(&cluster_id)
                    .expect("catalog out of sync")
                    .bound_objects
                    .insert(id);
            };
        }

        let entry = CatalogEntry {
            item,
            name,
            id,
            oid,
            used_by: Vec::new(),
            owner_id,
            privileges,
        };
        for u in &entry.uses().0 {
            match self.entry_by_id.get_mut(u) {
                Some(metadata) => metadata.used_by.push(entry.id),
                None => panic!(
                    "Catalog: missing dependent catalog item {} while installing {}",
                    &u,
                    self.resolve_full_name(&entry.name, entry.conn_id())
                ),
            }
        }
        let conn_id = entry.item().conn_id().unwrap_or(&SYSTEM_CONN_ID);
        let schema = self.get_schema_mut(
            &entry.name().qualifiers.database_spec,
            &entry.name().qualifiers.schema_spec,
            conn_id,
        );

        let prev_id = if let CatalogItem::Func(_) = entry.item() {
            schema.functions.insert(entry.name.item.clone(), entry.id)
        } else {
            schema.items.insert(entry.name.item.clone(), entry.id)
        };

        assert!(
            prev_id.is_none(),
            "builtin name collision on {:?}",
            entry.name.item.clone()
        );

        self.entry_by_id.insert(entry.id, entry.clone());
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub(super) fn drop_item(&mut self, id: GlobalId) {
        let metadata = self.entry_by_id.remove(&id).expect("catalog out of sync");
        if !metadata.item.is_placeholder() {
            info!(
                "drop {} {} ({})",
                metadata.item_type(),
                self.resolve_full_name(&metadata.name, metadata.conn_id()),
                id
            );
        }
        for u in &metadata.uses().0 {
            if let Some(dep_metadata) = self.entry_by_id.get_mut(u) {
                dep_metadata.used_by.retain(|u| *u != metadata.id)
            }
        }

        let conn_id = metadata.item.conn_id().unwrap_or(&SYSTEM_CONN_ID);
        let schema = self.get_schema_mut(
            &metadata.name().qualifiers.database_spec,
            &metadata.name().qualifiers.schema_spec,
            conn_id,
        );
        schema
            .items
            .remove(&metadata.name().item)
            .expect("catalog out of sync");

        if !id.is_system() {
            if let Some(cluster_id) = metadata.item.cluster_id() {
                assert!(
                    self.clusters_by_id
                        .get_mut(&cluster_id)
                        .expect("catalog out of sync")
                        .bound_objects
                        .remove(&id),
                    "catalog out of sync"
                );
            }
        }
    }

    /// Move item `id` into the bound objects of cluster `in_cluster`, removes it from the old
    /// cluster.
    ///
    /// Panics if
    /// * the item doesn't exist,
    /// * the item is not bound to the old cluster,
    /// * the new cluster doesn't exist,
    /// * the item is already bound to the new cluster.
    pub(super) fn move_item(&mut self, id: GlobalId, in_cluster: ClusterId) {
        let metadata = self.entry_by_id.get_mut(&id).expect("catalog out of sync");
        if let Some(cluster_id) = metadata.item.cluster_id() {
            assert!(
                self.clusters_by_id
                    .get_mut(&cluster_id)
                    .expect("catalog out of sync")
                    .bound_objects
                    .remove(&id),
                "catalog out of sync"
            );
        }
        assert!(
            self.clusters_by_id
                .get_mut(&in_cluster)
                .expect("catalog out of sync")
                .bound_objects
                .insert(id),
            "catalog out of sync"
        );
    }

    pub(super) fn get_database(&self, database_id: &DatabaseId) -> &Database {
        &self.database_by_id[database_id]
    }

    pub(super) fn get_database_mut(&mut self, database_id: &DatabaseId) -> &mut Database {
        self.database_by_id
            .get_mut(database_id)
            .expect("catalog out of sync")
    }

    pub(super) fn insert_cluster(
        &mut self,
        id: ClusterId,
        name: String,
        linked_object_id: Option<GlobalId>,
        introspection_source_indexes: Vec<(&'static BuiltinLog, GlobalId)>,
        owner_id: RoleId,
        privileges: PrivilegeMap,
        config: ClusterConfig,
    ) {
        let mut log_indexes = BTreeMap::new();
        for (log, index_id) in introspection_source_indexes {
            let source_name = FullItemName {
                database: RawDatabaseSpecifier::Ambient,
                schema: log.schema.into(),
                item: log.name.into(),
            };
            let index_name = format!("{}_{}_primary_idx", log.name, id);
            let mut index_name = QualifiedItemName {
                qualifiers: ItemQualifiers {
                    database_spec: ResolvedDatabaseSpecifier::Ambient,
                    schema_spec: SchemaSpecifier::Id(self.get_mz_internal_schema_id().clone()),
                },
                item: index_name.clone(),
            };
            index_name = self.find_available_name(index_name, &SYSTEM_CONN_ID);
            let index_item_name = index_name.item.clone();
            // TODO(clusters): Avoid panicking here on ID exhaustion
            // before stabilization.
            //
            // The OID counter is an i32, and could plausibly be exhausted.
            // Preallocating OIDs for each logging index is eminently
            // doable, but annoying enough that we don't bother now.
            let oid = self
                .allocate_oid()
                .unwrap_or_terminate("cannot return error here");
            let log_id = self.resolve_builtin_log(log);
            self.insert_item(
                index_id,
                oid,
                index_name,
                CatalogItem::Index(Index {
                    on: log_id,
                    keys: log
                        .variant
                        .index_by()
                        .into_iter()
                        .map(MirScalarExpr::Column)
                        .collect(),
                    create_sql: index_sql(
                        index_item_name,
                        id,
                        source_name,
                        &log.variant.desc(),
                        &log.variant.index_by(),
                    ),
                    conn_id: None,
                    resolved_ids: ResolvedIds(BTreeSet::from_iter([log_id])),
                    cluster_id: id,
                    is_retained_metrics_object: false,
                    custom_logical_compaction_window: None,
                }),
                MZ_SYSTEM_ROLE_ID,
                PrivilegeMap::default(),
            );
            log_indexes.insert(log.variant.clone(), index_id);
        }

        self.clusters_by_id.insert(
            id,
            Cluster {
                name: name.clone(),
                id,
                linked_object_id,
                bound_objects: BTreeSet::new(),
                log_indexes,
                replica_id_by_name_: BTreeMap::new(),
                replicas_by_id_: BTreeMap::new(),
                owner_id,
                privileges,
                config,
            },
        );
        assert!(self.clusters_by_name.insert(name, id).is_none());
        if let Some(linked_object_id) = linked_object_id {
            assert!(self
                .clusters_by_linked_object_id
                .insert(linked_object_id, id)
                .is_none());
        }
    }

    pub(super) fn rename_cluster(&mut self, id: ClusterId, to_name: String) {
        let cluster = self.get_cluster_mut(id);
        let old_name = std::mem::take(&mut cluster.name);
        cluster.name = to_name.clone();

        assert!(self.clusters_by_name.remove(&old_name).is_some());
        assert!(self.clusters_by_name.insert(to_name, id).is_none());
    }

    pub(super) fn insert_cluster_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_name: String,
        replica_id: ReplicaId,
        config: ReplicaConfig,
        owner_id: RoleId,
    ) {
        let replica = ClusterReplica {
            name: replica_name.clone(),
            cluster_id,
            replica_id,
            process_status: (0..config.location.num_processes())
                .map(|process_id| {
                    let status = ClusterReplicaProcessStatus {
                        status: ClusterStatus::NotReady(None),
                        time: to_datetime((self.config.now)()),
                    };
                    (u64::cast_from(process_id), status)
                })
                .collect(),
            config,
            owner_id,
        };
        let cluster = self
            .clusters_by_id
            .get_mut(&cluster_id)
            .expect("catalog out of sync");
        cluster.insert_replica(replica);
    }

    /// Renames a cluster replica.
    ///
    /// Panics if the cluster or cluster replica does not exist.
    pub(super) fn rename_cluster_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        to_name: String,
    ) {
        let cluster = self.get_cluster_mut(cluster_id);
        cluster.rename_replica(replica_id, to_name);
    }

    /// Inserts or updates the status of the specified cluster replica process.
    ///
    /// Panics if the cluster or replica does not exist.
    pub(super) fn ensure_cluster_status(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        process_id: ProcessId,
        status: ClusterReplicaProcessStatus,
    ) {
        let replica = self.get_cluster_replica_mut(cluster_id, replica_id);
        replica.process_status.insert(process_id, status);
    }

    /// Gets a reference to the specified replica of the specified cluster.
    ///
    /// Returns `None` if either the cluster or the replica does not
    /// exist.
    fn try_get_cluster_replica(
        &self,
        id: ClusterId,
        replica_id: ReplicaId,
    ) -> Option<&ClusterReplica> {
        self.try_get_cluster(id)
            .and_then(|cluster| cluster.replica(replica_id))
    }

    /// Gets a reference to the specified replica of the specified cluster.
    ///
    /// Panics if either the cluster or the replica does not exist.
    pub(super) fn get_cluster_replica(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) -> &ClusterReplica {
        self.try_get_cluster_replica(cluster_id, replica_id)
            .unwrap_or_else(|| panic!("unknown cluster replica: {cluster_id}.{replica_id}"))
    }

    /// Gets a mutable reference to the specified replica of the specified
    /// cluster.
    ///
    /// Returns `None` if either the clustere or the replica does not
    /// exist.
    fn try_get_cluster_replica_mut(
        &mut self,
        id: ClusterId,
        replica_id: ReplicaId,
    ) -> Option<&mut ClusterReplica> {
        self.try_get_cluster_mut(id)
            .and_then(|cluster| cluster.replica_mut(replica_id))
    }

    /// Gets a mutable reference to the specified replica of the specified
    /// cluster.
    ///
    /// Panics if either the cluster or the replica does not exist.
    fn get_cluster_replica_mut(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) -> &mut ClusterReplica {
        self.try_get_cluster_replica_mut(cluster_id, replica_id)
            .unwrap_or_else(|| panic!("unknown cluster replica: {cluster_id}.{replica_id}"))
    }

    /// Gets the status of the given cluster replica process.
    ///
    /// Panics if the cluster or replica does not exist
    pub(super) fn get_cluster_status(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        process_id: ProcessId,
    ) -> &ClusterReplicaProcessStatus {
        &self
            .get_cluster_replica(cluster_id, replica_id)
            .process_status[&process_id]
    }

    /// Get system configuration `name`.
    pub fn get_system_configuration(&self, name: &str) -> Result<&dyn Var, AdapterError> {
        Ok(self.system_configuration.get(name)?)
    }

    /// Set the default value for `name`, which is the value it will be reset to.
    pub(super) fn set_system_configuration_default(
        &mut self,
        name: &str,
        value: VarInput,
    ) -> Result<(), AdapterError> {
        Ok(self.system_configuration.set_default(name, value)?)
    }

    /// Insert system configuration `name` with `value`.
    ///
    /// Return a `bool` value indicating whether the configuration was modified
    /// by the call.
    pub(super) fn insert_system_configuration(
        &mut self,
        name: &str,
        value: VarInput,
    ) -> Result<bool, AdapterError> {
        Ok(self.system_configuration.set(name, value)?)
    }

    /// Reset system configuration `name`.
    ///
    /// Return a `bool` value indicating whether the configuration was modified
    /// by the call.
    pub(super) fn remove_system_configuration(&mut self, name: &str) -> Result<bool, AdapterError> {
        Ok(self.system_configuration.reset(name)?)
    }

    /// Remove all system configurations.
    pub(super) fn clear_system_configuration(&mut self) {
        self.system_configuration.reset_all();
    }

    /// Gets the schema map for the database matching `database_spec`.
    pub(super) fn resolve_schema_in_database(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_name: &str,
        conn_id: &ConnectionId,
    ) -> Result<&Schema, SqlCatalogError> {
        let schema = match database_spec {
            ResolvedDatabaseSpecifier::Ambient if schema_name == MZ_TEMP_SCHEMA => {
                self.temporary_schemas.get(conn_id)
            }
            ResolvedDatabaseSpecifier::Ambient => self
                .ambient_schemas_by_name
                .get(schema_name)
                .and_then(|id| self.ambient_schemas_by_id.get(id)),
            ResolvedDatabaseSpecifier::Id(id) => self.database_by_id.get(id).and_then(|db| {
                db.schemas_by_name
                    .get(schema_name)
                    .and_then(|id| db.schemas_by_id.get(id))
            }),
        };
        schema.ok_or_else(|| SqlCatalogError::UnknownSchema(schema_name.into()))
    }

    pub fn get_schema(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_spec: &SchemaSpecifier,
        conn_id: &ConnectionId,
    ) -> &Schema {
        // Keep in sync with `get_schemas_mut`
        match (database_spec, schema_spec) {
            (ResolvedDatabaseSpecifier::Ambient, SchemaSpecifier::Temporary) => {
                &self.temporary_schemas[conn_id]
            }
            (ResolvedDatabaseSpecifier::Ambient, SchemaSpecifier::Id(id)) => {
                &self.ambient_schemas_by_id[id]
            }

            (ResolvedDatabaseSpecifier::Id(database_id), SchemaSpecifier::Id(schema_id)) => {
                &self.database_by_id[database_id].schemas_by_id[schema_id]
            }
            (ResolvedDatabaseSpecifier::Id(_), SchemaSpecifier::Temporary) => {
                unreachable!("temporary schemas are in the ambient database")
            }
        }
    }

    pub(super) fn get_schema_mut(
        &mut self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_spec: &SchemaSpecifier,
        conn_id: &ConnectionId,
    ) -> &mut Schema {
        // Keep in sync with `get_schemas`
        match (database_spec, schema_spec) {
            (ResolvedDatabaseSpecifier::Ambient, SchemaSpecifier::Temporary) => self
                .temporary_schemas
                .get_mut(conn_id)
                .expect("catalog out of sync"),
            (ResolvedDatabaseSpecifier::Ambient, SchemaSpecifier::Id(id)) => self
                .ambient_schemas_by_id
                .get_mut(id)
                .expect("catalog out of sync"),
            (ResolvedDatabaseSpecifier::Id(database_id), SchemaSpecifier::Id(schema_id)) => self
                .database_by_id
                .get_mut(database_id)
                .expect("catalog out of sync")
                .schemas_by_id
                .get_mut(schema_id)
                .expect("catalog out of sync"),
            (ResolvedDatabaseSpecifier::Id(_), SchemaSpecifier::Temporary) => {
                unreachable!("temporary schemas are in the ambient database")
            }
        }
    }

    pub fn get_mz_catalog_schema_id(&self) -> &SchemaId {
        &self.ambient_schemas_by_name[MZ_CATALOG_SCHEMA]
    }

    pub fn get_pg_catalog_schema_id(&self) -> &SchemaId {
        &self.ambient_schemas_by_name[PG_CATALOG_SCHEMA]
    }

    pub fn get_information_schema_id(&self) -> &SchemaId {
        &self.ambient_schemas_by_name[INFORMATION_SCHEMA]
    }

    pub fn get_mz_internal_schema_id(&self) -> &SchemaId {
        &self.ambient_schemas_by_name[MZ_INTERNAL_SCHEMA]
    }

    pub fn is_system_schema(&self, schema: &str) -> bool {
        schema == MZ_CATALOG_SCHEMA
            || schema == PG_CATALOG_SCHEMA
            || schema == INFORMATION_SCHEMA
            || schema == MZ_INTERNAL_SCHEMA
    }

    pub fn is_system_schema_id(&self, id: &SchemaId) -> bool {
        id == self.get_mz_catalog_schema_id()
            || id == self.get_pg_catalog_schema_id()
            || id == self.get_information_schema_id()
            || id == self.get_mz_internal_schema_id()
    }

    pub fn is_system_schema_specifier(&self, spec: &SchemaSpecifier) -> bool {
        match spec {
            SchemaSpecifier::Temporary => false,
            SchemaSpecifier::Id(id) => self.is_system_schema_id(id),
        }
    }

    /// Optimized lookup for a builtin table
    ///
    /// Panics if the builtin table doesn't exist in the catalog
    pub fn resolve_builtin_table(&self, builtin: &'static BuiltinTable) -> GlobalId {
        self.resolve_builtin_object(&Builtin::<IdReference>::Table(builtin))
    }

    /// Optimized lookup for a builtin log
    ///
    /// Panics if the builtin log doesn't exist in the catalog
    pub fn resolve_builtin_log(&self, builtin: &'static BuiltinLog) -> GlobalId {
        self.resolve_builtin_object(&Builtin::<IdReference>::Log(builtin))
    }

    /// Optimized lookup for a builtin storage collection
    ///
    /// Panics if the builtin storage collection doesn't exist in the catalog
    pub fn resolve_builtin_source(&self, builtin: &'static BuiltinSource) -> GlobalId {
        self.resolve_builtin_object(&Builtin::<IdReference>::Source(builtin))
    }

    /// Optimized lookup for a builtin object
    ///
    /// Panics if the builtin object doesn't exist in the catalog
    pub fn resolve_builtin_object<T: TypeReference>(&self, builtin: &Builtin<T>) -> GlobalId {
        let schema_id = &self.ambient_schemas_by_name[builtin.schema()];
        let schema = &self.ambient_schemas_by_id[schema_id];
        schema.items[builtin.name()].clone()
    }

    pub fn config(&self) -> &mz_sql::catalog::CatalogConfig {
        &self.config
    }

    pub fn resolve_database(&self, database_name: &str) -> Result<&Database, SqlCatalogError> {
        match self.database_by_name.get(database_name) {
            Some(id) => Ok(&self.database_by_id[id]),
            None => Err(SqlCatalogError::UnknownDatabase(database_name.into())),
        }
    }

    pub fn resolve_schema(
        &self,
        current_database: Option<&DatabaseId>,
        database_name: Option<&str>,
        schema_name: &str,
        conn_id: &ConnectionId,
    ) -> Result<&Schema, SqlCatalogError> {
        let database_spec = match database_name {
            // If a database is explicitly specified, validate it. Note that we
            // intentionally do not validate `current_database` to permit
            // querying `mz_catalog` with an invalid session database, e.g., so
            // that you can run `SHOW DATABASES` to *find* a valid database.
            Some(database) => Some(ResolvedDatabaseSpecifier::Id(
                self.resolve_database(database)?.id().clone(),
            )),
            None => current_database.map(|id| ResolvedDatabaseSpecifier::Id(id.clone())),
        };

        // First try to find the schema in the named database.
        if let Some(database_spec) = database_spec {
            if let Ok(schema) =
                self.resolve_schema_in_database(&database_spec, schema_name, conn_id)
            {
                return Ok(schema);
            }
        }

        // Then fall back to the ambient database.
        if let Ok(schema) = self.resolve_schema_in_database(
            &ResolvedDatabaseSpecifier::Ambient,
            schema_name,
            conn_id,
        ) {
            return Ok(schema);
        }

        Err(SqlCatalogError::UnknownSchema(schema_name.into()))
    }

    pub fn resolve_search_path(
        &self,
        session: &Session,
    ) -> Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)> {
        let database = self
            .database_by_name
            .get(session.vars().database())
            .map(|id| id.clone());

        session
            .vars()
            .search_path()
            .iter()
            .map(|schema| {
                self.resolve_schema(database.as_ref(), None, schema.as_str(), session.conn_id())
            })
            .filter_map(|schema| schema.ok())
            .map(|schema| (schema.name().database.clone(), schema.id().clone()))
            .collect()
    }

    pub fn resolve_cluster(&self, name: &str) -> Result<&Cluster, SqlCatalogError> {
        let id = self
            .clusters_by_name
            .get(name)
            .ok_or_else(|| SqlCatalogError::UnknownCluster(name.to_string()))?;
        Ok(&self.clusters_by_id[id])
    }

    pub fn resolve_builtin_cluster(&self, cluster: &BuiltinCluster) -> &Cluster {
        let id = self
            .clusters_by_name
            .get(cluster.name)
            .expect("failed to lookup BuiltinCluster by name");
        self.clusters_by_id
            .get(id)
            .expect("failed to lookup BuiltinCluster by ID")
    }

    pub fn resolve_cluster_replica(
        &self,
        cluster_replica_name: &QualifiedReplica,
    ) -> Result<&ClusterReplica, SqlCatalogError> {
        let cluster = self.resolve_cluster(cluster_replica_name.cluster.as_str())?;
        let replica_name = cluster_replica_name.replica.as_str();
        let replica_id = cluster
            .replica_id(replica_name)
            .ok_or_else(|| SqlCatalogError::UnknownClusterReplica(replica_name.to_string()))?;
        Ok(cluster.replica(replica_id).expect("Must exist"))
    }

    /// Resolves [`PartialItemName`] into a [`CatalogEntry`].
    ///
    /// If `name` does not specify a database, the `current_database` is used.
    /// If `name` does not specify a schema, then the schemas in `search_path`
    /// are searched in order.
    #[allow(clippy::useless_let_if_seq)]
    pub fn resolve(
        &self,
        get_schema_entries: fn(&Schema) -> &BTreeMap<String, GlobalId>,
        current_database: Option<&DatabaseId>,
        search_path: &Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
        name: &PartialItemName,
        conn_id: &ConnectionId,
        err_gen: fn(String) -> SqlCatalogError,
    ) -> Result<&CatalogEntry, SqlCatalogError> {
        // If a schema name was specified, just try to find the item in that
        // schema. If no schema was specified, try to find the item in the connection's
        // temporary schema. If the item is not found, try to find the item in every
        // schema in the search path.
        let schemas = match &name.schema {
            Some(schema_name) => {
                match self.resolve_schema(
                    current_database,
                    name.database.as_deref(),
                    schema_name,
                    conn_id,
                ) {
                    Ok(schema) => vec![(schema.name.database.clone(), schema.id.clone())],
                    Err(e) => return Err(e),
                }
            }
            None => match self
                .get_schema(
                    &ResolvedDatabaseSpecifier::Ambient,
                    &SchemaSpecifier::Temporary,
                    conn_id,
                )
                .items
                .get(&name.item)
            {
                Some(id) => return Ok(self.get_entry(id)),
                None => search_path.to_vec(),
            },
        };

        for (database_spec, schema_spec) in schemas {
            let schema = self.get_schema(&database_spec, &schema_spec, conn_id);

            if let Some(id) = get_schema_entries(schema).get(&name.item) {
                return Ok(&self.entry_by_id[id]);
            }
        }
        Err(err_gen(name.to_string()))
    }

    /// Resolves `name` to a non-function [`CatalogEntry`].
    pub fn resolve_entry(
        &self,
        current_database: Option<&DatabaseId>,
        search_path: &Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
        name: &PartialItemName,
        conn_id: &ConnectionId,
    ) -> Result<&CatalogEntry, SqlCatalogError> {
        self.resolve(
            |schema| &schema.items,
            current_database,
            search_path,
            name,
            conn_id,
            SqlCatalogError::UnknownItem,
        )
    }

    /// Resolves `name` to a function [`CatalogEntry`].
    pub fn resolve_function(
        &self,
        current_database: Option<&DatabaseId>,
        search_path: &Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
        name: &PartialItemName,
        conn_id: &ConnectionId,
    ) -> Result<&CatalogEntry, SqlCatalogError> {
        self.resolve(
            |schema| &schema.functions,
            current_database,
            search_path,
            name,
            conn_id,
            |name| SqlCatalogError::UnknownFunction {
                name,
                alternative: None,
            },
        )
    }

    /// For an [`ObjectId`] gets the corresponding [`CommentObjectId`].
    pub(super) fn get_comment_id(&self, object_id: ObjectId) -> CommentObjectId {
        match object_id {
            ObjectId::Item(global_id) => {
                let entry = self.get_entry(&global_id);
                match entry.item_type() {
                    CatalogItemType::Table => CommentObjectId::Table(global_id),
                    CatalogItemType::Source => CommentObjectId::Source(global_id),
                    CatalogItemType::Sink => CommentObjectId::Sink(global_id),
                    CatalogItemType::View => CommentObjectId::View(global_id),
                    CatalogItemType::MaterializedView => {
                        CommentObjectId::MaterializedView(global_id)
                    }
                    CatalogItemType::Index => CommentObjectId::Index(global_id),
                    CatalogItemType::Func => CommentObjectId::Func(global_id),
                    CatalogItemType::Connection => CommentObjectId::Connection(global_id),
                    CatalogItemType::Type => CommentObjectId::Type(global_id),
                    CatalogItemType::Secret => CommentObjectId::Secret(global_id),
                }
            }
            ObjectId::Role(role_id) => CommentObjectId::Role(role_id),
            ObjectId::Database(database_id) => CommentObjectId::Database(database_id),
            ObjectId::Schema((database, schema)) => CommentObjectId::Schema((database, schema)),
            ObjectId::Cluster(cluster_id) => CommentObjectId::Cluster(cluster_id),
            ObjectId::ClusterReplica(cluster_replica_id) => {
                CommentObjectId::ClusterReplica(cluster_replica_id)
            }
        }
    }

    /// Return current system configuration.
    pub fn system_config(&self) -> &SystemVars {
        &self.system_configuration
    }

    /// Serializes the catalog's in-memory state.
    ///
    /// There are no guarantees about the format of the serialized state, except
    /// that the serialized state for two identical catalogs will compare
    /// identically.
    pub fn dump(&self) -> Result<String, Error> {
        serde_json::to_string_pretty(&self).map_err(|e| {
            Error::new(ErrorKind::Unstructured(format!(
                // Don't panic here because we don't have compile-time failures for maps with
                // non-string keys.
                "internal error: could not dump catalog: {}",
                e
            )))
        })
    }

    pub fn availability_zones(&self) -> &[String] {
        &self.availability_zones
    }

    /// Returns the default storage cluster size .
    ///
    /// If a default size was given as configuration, it is always used,
    /// otherwise the smallest size is used instead.
    pub fn default_linked_cluster_size(&self) -> String {
        match &self.default_storage_cluster_size {
            Some(default_storage_cluster_size) => default_storage_cluster_size.clone(),
            None => {
                let (size, _allocation) = self
                    .cluster_replica_sizes
                    .enabled_allocations()
                    .min_by_key(|(_, a)| (a.scale, a.workers, a.memory_limit))
                    .expect("should have at least one valid cluster replica size");
                size.clone()
            }
        }
    }

    pub fn ensure_not_reserved_role(&self, role_id: &RoleId) -> Result<(), Error> {
        if role_id.is_system() || role_id.is_public() {
            let role = self.get_role(role_id);
            Err(Error::new(ErrorKind::ReservedRoleName(
                role.name().to_string(),
            )))
        } else {
            Ok(())
        }
    }

    pub fn ensure_not_system_role(&self, role_id: &RoleId) -> Result<(), Error> {
        if role_id.is_system() {
            let role = self.get_role(role_id);
            Err(Error::new(ErrorKind::ReservedSystemRoleName(
                role.name().to_string(),
            )))
        } else {
            Ok(())
        }
    }

    // TODO(mjibson): Is there a way to make this a closure to avoid explicitly
    // passing tx, session, and builtin_table_updates?
    pub(crate) fn add_to_audit_log(
        &self,
        oracle_write_ts: mz_repr::Timestamp,
        session: Option<&ConnMeta>,
        tx: &mut mz_catalog::Transaction,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
        audit_events: &mut Vec<VersionedEvent>,
        event_type: EventType,
        object_type: ObjectType,
        details: EventDetails,
    ) -> Result<(), Error> {
        let user = session.map(|session| session.user().name.to_string());

        // unsafe_mock_audit_event_timestamp can only be set to Some when running in unsafe mode.

        let occurred_at = match self
            .system_configuration
            .unsafe_mock_audit_event_timestamp()
        {
            Some(ts) => ts.into(),
            _ => oracle_write_ts.into(),
        };
        let id = tx.get_and_increment_id(mz_catalog::AUDIT_LOG_ID_ALLOC_KEY.to_string())?;
        let event = VersionedEvent::new(id, event_type, object_type, details, user, occurred_at);
        builtin_table_updates.push(self.pack_audit_log_update(&event)?);
        audit_events.push(event.clone());
        tx.insert_audit_log_event(event);
        Ok(())
    }

    pub(super) fn add_to_storage_usage(
        &self,
        tx: &mut mz_catalog::Transaction,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
        shard_id: Option<String>,
        size_bytes: u64,
        collection_timestamp: EpochMillis,
    ) -> Result<(), Error> {
        let id = tx.get_and_increment_id(mz_catalog::STORAGE_USAGE_ID_ALLOC_KEY.to_string())?;

        let details = VersionedStorageUsage::new(id, shard_id, size_bytes, collection_timestamp);
        builtin_table_updates.push(self.pack_storage_usage_update(&details)?);
        tx.insert_storage_usage_event(details);
        Ok(())
    }

    pub(super) fn get_owner_id(&self, id: &ObjectId, conn_id: &ConnectionId) -> Option<RoleId> {
        match id {
            ObjectId::Cluster(id) => Some(self.get_cluster(*id).owner_id()),
            ObjectId::ClusterReplica((cluster_id, replica_id)) => Some(
                self.get_cluster_replica(*cluster_id, *replica_id)
                    .owner_id(),
            ),
            ObjectId::Database(id) => Some(self.get_database(id).owner_id()),
            ObjectId::Schema((database_spec, schema_spec)) => Some(
                self.get_schema(database_spec, schema_spec, conn_id)
                    .owner_id(),
            ),
            ObjectId::Item(id) => Some(*self.get_entry(id).owner_id()),
            ObjectId::Role(_) => None,
        }
    }

    pub(super) fn get_object_type(&self, object_id: &ObjectId) -> mz_sql::catalog::ObjectType {
        match object_id {
            ObjectId::Cluster(_) => mz_sql::catalog::ObjectType::Cluster,
            ObjectId::ClusterReplica(_) => mz_sql::catalog::ObjectType::ClusterReplica,
            ObjectId::Database(_) => mz_sql::catalog::ObjectType::Database,
            ObjectId::Schema(_) => mz_sql::catalog::ObjectType::Schema,
            ObjectId::Role(_) => mz_sql::catalog::ObjectType::Role,
            ObjectId::Item(id) => self.get_entry(id).item_type().into(),
        }
    }

    pub(super) fn get_system_object_type(
        &self,
        id: &SystemObjectId,
    ) -> mz_sql::catalog::SystemObjectType {
        match id {
            SystemObjectId::Object(object_id) => {
                SystemObjectType::Object(self.get_object_type(object_id))
            }
            SystemObjectId::System => SystemObjectType::System,
        }
    }
}

impl ConnectionResolver for CatalogState {
    fn resolve_connection(
        &self,
        id: GlobalId,
    ) -> mz_storage_types::connections::Connection<InlinedConnection> {
        use mz_storage_types::connections::Connection::*;
        match self
            .get_entry(&id)
            .connection()
            .expect("catalog out of sync")
            .connection
            .clone()
        {
            Kafka(conn) => Kafka(conn.into_inline_connection(self)),
            Postgres(conn) => Postgres(conn.into_inline_connection(self)),
            Csr(conn) => Csr(conn.into_inline_connection(self)),
            Ssh(conn) => Ssh(conn),
            Aws(conn) => Aws(conn),
            AwsPrivatelink(conn) => AwsPrivatelink(conn),
        }
    }
}