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
use std::sync::Arc;

use itertools::Itertools;
use log::error;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::meta::scale_task::TaskType;
use risingwave_pb::meta::TaskStatus;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::Sender;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

use crate::model::{MetadataModel, ScaleTask, ScaleTaskId};
use crate::storage::MetaStore;

pub type ScaleManagerRef<S> = Arc<ScaleManager<S>>;
type ScaleTaskCacheRef = Arc<Mutex<HashMap<ScaleTaskId, ScaleTask>>>;

/// [`ScaleManager`] is used to receive scale tasks.
#[allow(dead_code)]
pub struct ScaleManager<S: MetaStore> {
    meta_store: Arc<S>,

    task_cache: ScaleTaskCacheRef,
    task_queue_tx: UnboundedSender<ScaleTaskId>,
}

impl<S> ScaleManager<S>
where
    S: MetaStore,
{
    pub async fn new(meta_store: Arc<S>) -> Result<(Self, JoinHandle<()>, Sender<()>)> {
        let tasks = ScaleTask::list(&*meta_store).await?;
        let (task_queue_tx, mut task_queue_rx) = mpsc::unbounded_channel();

        tasks
            .iter()
            .filter(|task| task.task_status == TaskStatus::Pending)
            .sorted_by(|t1, t2| t1.task_create_time.cmp(&t2.task_create_time))
            .map(|task| task.task_id.clone())
            .for_each(|task_id| {
                task_queue_tx.send(task_id).unwrap();
            });
        let task_cache = Arc::new(Mutex::new(
            tasks
                .into_iter()
                .map(|task| (task.task_id.clone(), task))
                .collect(),
        ));

        let scale_manager = ScaleManager {
            meta_store: meta_store.clone(),
            task_cache: task_cache.clone(),
            task_queue_tx,
        };

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            loop {
                let task_id = tokio::select! {
                    task_id = task_queue_rx.recv() => {
                        match task_id {
                            None => return,
                            Some(task_id) => task_id
                        }
                    }
                    _ = &mut shutdown_rx => {
                        tracing::info!("Scale Manager is stopped");
                        return;
                    }
                };
                if let Err(err) =
                    Self::solve_task(meta_store.clone(), task_cache.clone(), task_id.clone()).await
                {
                    error!("Failed to solve scale task: {}", err);
                    let mut task_cache_guard = task_cache.lock().await;
                    if let Some(task) = task_cache_guard.get_mut(&task_id) {
                        task.end(&*meta_store, TaskStatus::Failed).await.ok();
                    }
                }
            }
        });

        Ok((scale_manager, join_handle, shutdown_tx))
    }

    async fn solve_task(
        meta_store: Arc<S>,
        task_cache: ScaleTaskCacheRef,
        task_id: ScaleTaskId,
    ) -> Result<()> {
        let mut task_cache_guard = task_cache.lock().await;
        let mut task = task_cache_guard.get_mut(&task_id).unwrap();
        if TaskStatus::Cancelled == task.task_status {
            return Ok(());
        }

        match task.task_type {
            TaskType::Invalid => unreachable!(),
            TaskType::ScaleIn => {
                task.status(&*meta_store, TaskStatus::Building).await?;
                let _hosts = task.hosts.clone();
                drop(task_cache_guard);

                // TODO: call some method to scale in

                task_cache_guard = task_cache.lock().await;
                task = task_cache_guard.get_mut(&task_id).unwrap();
                task.end(&*meta_store, TaskStatus::Finished).await?;
            }
            TaskType::ScaleOut => {
                let _hosts = task.hosts.clone();
                let _fragment_parallelism = task.fragment_parallelism.clone();
                drop(task_cache_guard);

                // TODO: call some method and wait before CN starts.
                // Temporarily replace them with `sleep` in unit tests.
                #[cfg(test)]
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;

                task_cache_guard = task_cache.lock().await;
                task = task_cache_guard.get_mut(&task_id).unwrap();
                if TaskStatus::Cancelled == task.task_status {
                    return Ok(());
                }
                task.status(&*meta_store, TaskStatus::Building).await?;
                drop(task_cache_guard);

                // TODO: call some method to scale out
                // Temporarily replace them with `sleep` in unit tests.
                #[cfg(test)]
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;

                task_cache_guard = task_cache.lock().await;
                task = task_cache_guard.get_mut(&task_id).unwrap();
                task.end(&*meta_store, TaskStatus::Finished).await?;
            }
        }

        Ok(())
    }

    pub async fn add_scale_task(&self, mut task: ScaleTask) -> Result<ScaleTaskId> {
        // Make sure tasks in task_queue are sorted in ascending order by task_id.
        let mut task_cache_guard = self.task_cache.lock().await;
        task.init(&*self.meta_store).await?;
        let task_id = task.task_id.clone();
        task_cache_guard.insert(task_id.clone(), task);
        self.task_queue_tx.send(task_id.clone()).unwrap();
        Ok(task_id)
    }

    pub async fn get_task_status(&self, task_id: ScaleTaskId) -> TaskStatus {
        self.task_cache
            .lock()
            .await
            .get(&task_id)
            .map_or(TaskStatus::NotFound, |task| task.task_status)
    }

    pub async fn abort_task(&self, task_id: ScaleTaskId) -> Result<()> {
        let mut task_cache_guard = self.task_cache.lock().await;
        if let Some(task) = task_cache_guard.get_mut(&task_id) {
            match task.task_status {
                TaskStatus::Pending => {
                    task.end(&*self.meta_store, TaskStatus::Cancelled).await?;
                    Ok(())
                }
                status => Err(RwError::from(ErrorCode::InternalError(format!(
                    "TaskStatus: {:?}",
                    status
                )))),
            }
        } else {
            Err(RwError::from(ErrorCode::InternalError(
                "Task not found!".to_string(),
            )))
        }
    }

    pub async fn remove_task(&self, task_id: ScaleTaskId) -> Result<()> {
        let mut task_cache_guard = self.task_cache.lock().await;
        if let Some(task) = task_cache_guard.get_mut(&task_id) {
            match task.task_status {
                TaskStatus::Cancelled | TaskStatus::Finished | TaskStatus::Failed => {
                    task_cache_guard.remove(&task_id);
                    ScaleTask::delete(&*self.meta_store, &task_id).await?;
                    Ok(())
                }
                _ => Err(RwError::from(ErrorCode::InternalError(
                    "Task is being executed.".to_string(),
                ))),
            }
        } else {
            Err(RwError::from(ErrorCode::InternalError(
                "Task not found!".to_string(),
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::MetaSrvEnv;

    #[tokio::test]
    async fn test_scale_out_task() -> Result<()> {
        let env = MetaSrvEnv::for_test().await;
        let (scale_manager, scale_handle, scale_shutdown) =
            ScaleManager::new(env.meta_store_ref()).await?;

        let task1 = ScaleTask {
            task_id: String::new(),
            task_type: TaskType::ScaleOut,
            hosts: vec![],
            fragment_parallelism: HashMap::new(),
            task_status: TaskStatus::NotFound,
            task_create_time: 0,
            task_end_time: 0,
        };
        let task2 = task1.clone();
        let task3 = task1.clone();

        let task1_id = scale_manager.add_scale_task(task1).await?;
        let task2_id = scale_manager.add_scale_task(task2).await?;
        let task3_id = scale_manager.add_scale_task(task3).await?;

        scale_manager.abort_task(task2_id.clone()).await?;
        assert_eq!(
            TaskStatus::Pending,
            scale_manager.get_task_status(task1_id.clone()).await
        );
        assert_eq!(
            TaskStatus::Cancelled,
            scale_manager.get_task_status(task2_id).await
        );

        scale_manager.abort_task(task1_id.clone()).await?;
        assert_eq!(
            TaskStatus::Cancelled,
            scale_manager.get_task_status(task1_id).await
        );

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        scale_shutdown.send(()).unwrap();
        scale_handle.await?;
        assert_eq!(
            TaskStatus::Finished,
            scale_manager.get_task_status(task3_id).await
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_scale_manager_recover() -> Result<()> {
        let env = MetaSrvEnv::for_test().await;

        let task_id = uuid::Uuid::new_v4().to_string();
        let task = ScaleTask {
            task_id: task_id.clone(),
            task_type: TaskType::ScaleOut,
            hosts: vec![],
            fragment_parallelism: HashMap::new(),
            task_status: TaskStatus::Finished,
            task_create_time: 0,
            task_end_time: 0,
        };
        task.insert(env.meta_store()).await?;

        let (scale_manager, scale_handle, scale_shutdown) =
            ScaleManager::new(env.meta_store_ref()).await?;

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        scale_shutdown.send(()).unwrap();
        scale_handle.await?;

        assert_eq!(
            TaskStatus::Finished,
            scale_manager.get_task_status(task_id).await
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_abort_and_remove_finished_task() -> Result<()> {
        let env = MetaSrvEnv::for_test().await;
        let (scale_manager, scale_handle, scale_shutdown) =
            ScaleManager::new(env.meta_store_ref()).await?;

        let task = ScaleTask {
            task_id: String::new(),
            task_type: TaskType::ScaleOut,
            hosts: vec![],
            fragment_parallelism: HashMap::new(),
            task_status: TaskStatus::NotFound,
            task_create_time: 0,
            task_end_time: 0,
        };
        let task_id = scale_manager.add_scale_task(task).await?;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        scale_shutdown.send(()).unwrap();
        scale_handle.await?;

        assert_eq!(
            TaskStatus::Finished,
            scale_manager.get_task_status(task_id.clone()).await
        );

        scale_manager
            .abort_task(task_id.clone())
            .await
            .expect_err("task should panic");
        assert_eq!(
            TaskStatus::Finished,
            scale_manager.get_task_status(task_id.clone()).await
        );
        assert_eq!(
            TaskStatus::Finished,
            ScaleTask::select(env.meta_store(), &task_id)
                .await?
                .unwrap()
                .task_status
        );

        scale_manager.remove_task(task_id.clone()).await?;
        assert_eq!(
            TaskStatus::NotFound,
            scale_manager.get_task_status(task_id.clone()).await
        );
        assert!(ScaleTask::select(env.meta_store(), &task_id)
            .await?
            .is_none());

        Ok(())
    }
}
