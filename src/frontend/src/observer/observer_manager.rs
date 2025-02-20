// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use parking_lot::RwLock;
use risingwave_common::catalog::CatalogVersion;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common_service::observer_manager::ObserverNodeImpl;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::SubscribeResponse;
use tokio::sync::watch::Sender;

use crate::catalog::root_catalog::Catalog;
use crate::scheduler::worker_node_manager::WorkerNodeManagerRef;
use crate::scheduler::HummockSnapshotManagerRef;
use crate::user::user_manager::UserInfoManager;
use crate::user::UserInfoVersion;

pub(crate) struct FrontendObserverNode {
    worker_node_manager: WorkerNodeManagerRef,
    catalog: Arc<RwLock<Catalog>>,
    catalog_updated_tx: Sender<CatalogVersion>,
    user_info_manager: Arc<RwLock<UserInfoManager>>,
    user_info_updated_tx: Sender<UserInfoVersion>,
}

impl ObserverNodeImpl for FrontendObserverNode {
    fn handle_notification(&mut self, resp: SubscribeResponse) {
        let Some(info) = resp.info.as_ref() else {
            return;
        };

        match info {
            Info::Database(_)
            | Info::Schema(_)
            | Info::Table(_)
            | Info::Source(_)
            | Info::Sink(_) => {
                self.handle_catalog_notification(resp);
            }
            Info::Node(node) => {
                self.update_worker_node_manager(resp.operation(), node.clone());
            }
            Info::User(_) => {
                self.handle_user_notification(resp);
            }
            Info::Snapshot(_) => {
                panic!(
                    "receiving a snapshot in the middle is unsupported now {:?}",
                    resp
                )
            }
            Info::HummockSnapshot(_) => {
                // TODO: remove snapshot notify
            }
        }
    }

    fn handle_initialization_notification(&mut self, resp: SubscribeResponse) -> Result<()> {
        let mut catalog_guard = self.catalog.write();
        let mut user_guard = self.user_info_manager.write();
        catalog_guard.clear();
        user_guard.clear();
        match resp.info {
            Some(Info::Snapshot(snapshot)) => {
                for db in snapshot.database {
                    catalog_guard.create_database(db)
                }
                for schema in snapshot.schema {
                    catalog_guard.create_schema(schema)
                }
                for table in snapshot.table {
                    catalog_guard.create_table(&table)
                }
                for source in snapshot.source {
                    catalog_guard.create_source(source)
                }
                for user in snapshot.users {
                    user_guard.create_user(user)
                }
                self.worker_node_manager.refresh_worker_node(snapshot.nodes);
            }
            _ => {
                return Err(ErrorCode::InternalError(format!(
                    "the first notify should be frontend snapshot, but get {:?}",
                    resp
                ))
                .into())
            }
        }
        catalog_guard.set_version(resp.version);
        self.catalog_updated_tx.send(resp.version).unwrap();
        Ok(())
    }
}
impl FrontendObserverNode {
    pub fn new(
        worker_node_manager: WorkerNodeManagerRef,
        catalog: Arc<RwLock<Catalog>>,
        catalog_updated_tx: Sender<CatalogVersion>,
        user_info_manager: Arc<RwLock<UserInfoManager>>,
        user_info_updated_tx: Sender<UserInfoVersion>,
        _hummock_snapshot_manager: HummockSnapshotManagerRef,
    ) -> Self {
        Self {
            worker_node_manager,
            catalog,
            catalog_updated_tx,
            user_info_manager,
            user_info_updated_tx,
        }
    }

    fn handle_catalog_notification(&mut self, resp: SubscribeResponse) {
        let Some(info) = resp.info.as_ref() else {
            return;
        };

        let mut catalog_guard = self.catalog.write();
        match info {
            Info::Database(database) => match resp.operation() {
                Operation::Add => catalog_guard.create_database(database.clone()),
                Operation::Delete => catalog_guard.drop_database(database.id),
                _ => panic!("receive an unsupported notify {:?}", resp.clone()),
            },
            Info::Schema(schema) => match resp.operation() {
                Operation::Add => catalog_guard.create_schema(schema.clone()),
                Operation::Delete => catalog_guard.drop_schema(schema.database_id, schema.id),
                _ => panic!("receive an unsupported notify {:?}", resp),
            },
            Info::Table(table) => match resp.operation() {
                Operation::Add => catalog_guard.create_table(table),
                Operation::Delete => {
                    catalog_guard.drop_table(table.database_id, table.schema_id, table.id.into())
                }
                Operation::Update => catalog_guard.update_table(table),
                _ => panic!("receive an unsupported notify {:?}", resp),
            },
            Info::Source(source) => match resp.operation() {
                Operation::Add => catalog_guard.create_source(source.clone()),
                Operation::Delete => {
                    catalog_guard.drop_source(source.database_id, source.schema_id, source.id)
                }
                _ => panic!("receive an unsupported notify {:?}", resp),
            },
            _ => unreachable!(),
        }
        assert!(
            resp.version > catalog_guard.version(),
            "resp version={:?}, current version={:?}",
            resp.version,
            catalog_guard.version()
        );
        catalog_guard.set_version(resp.version);
        self.catalog_updated_tx.send(resp.version).unwrap();
    }

    fn handle_user_notification(&mut self, resp: SubscribeResponse) {
        let Some(info) = resp.info.as_ref() else {
            return;
        };

        let mut user_guard = self.user_info_manager.write();
        match info {
            Info::User(user) => match resp.operation() {
                Operation::Add => user_guard.create_user(user.clone()),
                Operation::Delete => user_guard.drop_user(&user.name),
                Operation::Update => user_guard.update_user(user.clone()),
                _ => panic!("receive an unsupported notify {:?}", resp),
            },
            _ => unreachable!(),
        }
        assert!(
            resp.version > user_guard.version(),
            "resp version={:?}, current version={:?}",
            resp.version,
            user_guard.version()
        );
        user_guard.set_version(resp.version);
        self.user_info_updated_tx.send(resp.version).unwrap();
    }

    /// `update_worker_node_manager` is called in `start` method.
    /// It calls `add_worker_node` and `remove_worker_node` of `WorkerNodeManager`.
    fn update_worker_node_manager(&self, operation: Operation, node: WorkerNode) {
        tracing::debug!(
            "Update worker nodes, operation: {:?}, node: {:?}",
            operation,
            node
        );

        match operation {
            Operation::Add => self.worker_node_manager.add_worker_node(node),
            Operation::Delete => self.worker_node_manager.remove_worker_node(node),
            _ => (),
        }
    }
}
