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

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use futures::FutureExt;
use parking_lot::RwLock;
use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::slice_transform::SliceTransform;
use risingwave_hummock_sdk::{get_local_sst_id, HummockEpoch, LocalSstableInfo};
use risingwave_pb::hummock::SstableInfo;
use risingwave_rpc_client::HummockMetaClient;

use crate::hummock::compaction_executor::CompactionExecutor;
use crate::hummock::compactor::{get_remote_sstable_id_generator, Compactor, CompactorContext};
use crate::hummock::conflict_detector::ConflictDetector;
use crate::hummock::shared_buffer::OrderSortedUncommittedData;
use crate::hummock::{HummockResult, SstableStoreRef};
use crate::monitor::StateStoreMetrics;

pub(crate) type UploadTaskPayload = OrderSortedUncommittedData;
pub(crate) type UploadTaskResult = HummockResult<Vec<LocalSstableInfo>>;

pub struct SharedBufferUploader {
    options: Arc<StorageConfig>,
    write_conflict_detector: Option<Arc<ConflictDetector>>,

    sstable_store: SstableStoreRef,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
    next_local_sstable_id: Arc<AtomicU64>,
    stats: Arc<StateStoreMetrics>,
    compaction_executor: Option<Arc<CompactionExecutor>>,
    local_object_store_compactor_context: Arc<CompactorContext>,
    remote_object_store_compactor_context: Arc<CompactorContext>,
}

impl SharedBufferUploader {
    pub fn new(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        stats: Arc<StateStoreMetrics>,
        write_conflict_detector: Option<Arc<ConflictDetector>>,
        table_id_to_slice_transform: Arc<RwLock<HashMap<u32, Arc<dyn SliceTransform>>>>,
    ) -> Self {
        let compaction_executor = if options.share_buffer_compaction_worker_threads_number == 0 {
            None
        } else {
            Some(Arc::new(CompactionExecutor::new(Some(
                options.share_buffer_compaction_worker_threads_number as usize,
            ))))
        };
        let next_local_sstable_id = Arc::new(AtomicU64::new(0));
        let local_object_store_compactor_context = Arc::new(CompactorContext {
            options: options.clone(),
            hummock_meta_client: hummock_meta_client.clone(),
            sstable_store: sstable_store.clone(),
            stats: stats.clone(),
            is_share_buffer_compact: true,
            sstable_id_generator: {
                let atomic = next_local_sstable_id.clone();
                Arc::new(move || {
                    {
                        let atomic = atomic.clone();
                        async move { Ok(get_local_sst_id(atomic.fetch_add(1, Relaxed))) }
                    }
                    .boxed()
                })
            },
            compaction_executor: compaction_executor.as_ref().cloned(),
            table_id_to_slice_transform: table_id_to_slice_transform.clone(),
        });
        let remote_object_store_compactor_context = Arc::new(CompactorContext {
            options: options.clone(),
            hummock_meta_client: hummock_meta_client.clone(),
            sstable_store: sstable_store.clone(),
            stats: stats.clone(),
            is_share_buffer_compact: true,
            sstable_id_generator: get_remote_sstable_id_generator(hummock_meta_client.clone()),
            compaction_executor: compaction_executor.as_ref().cloned(),
            table_id_to_slice_transform: table_id_to_slice_transform.clone(),
        });
        Self {
            options,
            write_conflict_detector,
            sstable_store,
            hummock_meta_client,
            next_local_sstable_id,
            stats,
            compaction_executor,
            local_object_store_compactor_context,
            remote_object_store_compactor_context,
        }
    }
}

impl SharedBufferUploader {
    pub async fn flush(
        &self,
        _epoch: HummockEpoch,
        is_local: bool,
        payload: UploadTaskPayload,
    ) -> HummockResult<Vec<LocalSstableInfo>> {
        if payload.is_empty() {
            return Ok(vec![]);
        }

        // Compact buffers into SSTs
        let mem_compactor_ctx = if is_local {
            self.local_object_store_compactor_context.clone()
        } else {
            self.remote_object_store_compactor_context.clone()
        };

        let tables =
            Compactor::compact_shared_buffer_by_compaction_group(mem_compactor_ctx, payload)
                .await?;

        let uploaded_sst_info = tables
            .into_iter()
            .map(|(compaction_group_id, sst, table_ids)| {
                (
                    compaction_group_id,
                    SstableInfo {
                        id: sst.id,
                        key_range: Some(risingwave_pb::hummock::KeyRange {
                            left: sst.meta.smallest_key.clone(),
                            right: sst.meta.largest_key.clone(),
                            inf: false,
                        }),
                        file_size: sst.meta.estimated_size as u64,
                        table_ids,
                    },
                )
            })
            .collect();

        // TODO: re-enable conflict detector after we have a better way to determine which actor
        // writes the batch. if let Some(detector) = &self.write_conflict_detector {
        //     for data_list in payload {
        //         for data in data_list {
        //             if let UncommittedData::Batch(batch) = data {
        //                 detector.check_conflict_and_track_write_batch(batch.get_payload(),
        // epoch);             }
        //         }
        //     }
        // }

        Ok(uploaded_sst_info)
    }
}
