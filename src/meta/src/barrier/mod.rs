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

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet, VecDeque};
use std::iter::once;
use std::sync::atomic::AtomicBool;
use std::sync::{atomic, Arc};
use std::time::Duration;

use futures::future::try_join_all;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::error::{ErrorCode, Result, RwError, ToRwResult};
use risingwave_common::util::epoch::{Epoch, INVALID_EPOCH};
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_pb::common::worker_node::State::Running;
use risingwave_pb::common::WorkerType;
use risingwave_pb::data::Barrier;
use risingwave_pb::stream_service::{InjectBarrierRequest, InjectBarrierResponse};
use smallvec::SmallVec;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{oneshot, watch, RwLock};
use tokio::task::JoinHandle;
use uuid::Uuid;

pub use self::command::Command;
use self::command::CommandContext;
use self::info::BarrierActorInfo;
use self::notifier::{Notifier, UnfinishedNotifiers};
use crate::cluster::{ClusterManagerRef, META_NODE_ID};
use crate::hummock::HummockManagerRef;
use crate::manager::{CatalogManagerRef, MetaSrvEnv};
use crate::model::{ActorId, BarrierManagerState};
use crate::rpc::metrics::MetaMetrics;
use crate::storage::MetaStore;
use crate::stream::FragmentManagerRef;

mod command;
mod info;
mod notifier;
mod recovery;

type Scheduled = (Command, SmallVec<[Notifier; 1]>);

#[derive(Debug)]
enum BarrierSendResult {
    Ok(
        Epoch,
        HashSet<ActorId>,
        SmallVec<[Notifier; 1]>,
        Vec<InjectBarrierResponse>,
    ),
    Err(Command, RwError, bool),
}

/// A buffer or queue for scheduling barriers.
struct ScheduledBarriers {
    buffer: RwLock<VecDeque<Scheduled>>,

    /// When `buffer` is not empty anymore, all subscribers of this watcher will be notified.
    changed_tx: watch::Sender<()>,
}

impl ScheduledBarriers {
    fn new() -> Self {
        Self {
            buffer: RwLock::new(VecDeque::new()),
            changed_tx: watch::channel(()).0,
        }
    }

    /// Pop a scheduled barrier from the buffer, or a default checkpoint barrier if not exists.
    async fn pop_or_default(&self) -> Scheduled {
        let mut buffer = self.buffer.write().await;

        // If no command scheduled, create periodic checkpoint barrier by default.
        buffer
            .pop_front()
            .unwrap_or_else(|| (Command::checkpoint(), Default::default()))
    }

    /// Wait for at least one scheduled barrier in the buffer.
    async fn wait_one(&self) {
        let buffer = self.buffer.read().await;
        if buffer.len() > 0 {
            return;
        }
        let mut rx = self.changed_tx.subscribe();
        drop(buffer);

        rx.changed().await.unwrap();
    }

    /// Push a scheduled barrier into the buffer.
    async fn push(&self, scheduled: Scheduled) {
        let mut buffer = self.buffer.write().await;
        buffer.push_back(scheduled);
        if buffer.len() == 1 {
            self.changed_tx.send(()).ok();
        }
    }

    /// Attach `new_notifiers` to the very first scheduled barrier. If there's no one scheduled, a
    /// default checkpoint barrier will be created.
    async fn attach_notifiers(&self, new_notifiers: impl IntoIterator<Item = Notifier>) {
        let mut buffer = self.buffer.write().await;
        match buffer.front_mut() {
            Some((_, notifiers)) => notifiers.extend(new_notifiers),
            None => {
                // If no command scheduled, create periodic checkpoint barrier by default.
                buffer.push_back((Command::checkpoint(), new_notifiers.into_iter().collect()));
                if buffer.len() == 1 {
                    self.changed_tx.send(()).ok();
                }
            }
        }
    }

    /// Clear all buffered scheduled barriers, and notify their subscribers with failed as aborted.
    async fn abort(&self) {
        let mut buffer = self.buffer.write().await;
        while let Some((_, notifiers)) = buffer.pop_front() {
            notifiers.into_iter().for_each(|notify| {
                notify.notify_collection_failed(RwError::from(ErrorCode::InternalError(
                    "Scheduled barrier abort.".to_string(),
                )))
            })
        }
    }
}

/// [`crate::barrier::GlobalBarrierManager`] sends barriers to all registered compute nodes and
/// collect them, with monotonic increasing epoch numbers. On compute nodes, `LocalBarrierManager`
/// in `risingwave_stream` crate will serve these requests and dispatch them to source actors.
///
/// Configuration change in our system is achieved by the mutation in the barrier. Thus,
/// [`crate::barrier::GlobalBarrierManager`] provides a set of interfaces like a state machine,
/// accepting [`Command`] that carries info to build `Mutation`. To keep the consistency between
/// barrier manager and meta store, some actions like "drop materialized view" or "create mv on mv"
/// must be done in barrier manager transactional using [`Command`].
pub struct GlobalBarrierManager<S: MetaStore> {
    /// The maximal interval for sending a barrier.
    interval: Duration,

    /// Enable recovery or not when failover.
    enable_recovery: bool,

    /// The queue of scheduled barriers.
    scheduled_barriers: ScheduledBarriers,

    cluster_manager: ClusterManagerRef<S>,

    catalog_manager: CatalogManagerRef<S>,

    fragment_manager: FragmentManagerRef<S>,

    hummock_manager: HummockManagerRef<S>,

    metrics: Arc<MetaMetrics>,

    env: MetaSrvEnv<S>,

    epoch_heap: Arc<RwLock<BinaryHeap<HeapNode>>>,

    wait_recovery: Arc<AtomicBool>,

    abort_command: Arc<RwLock<Vec<Command>>>,

    semaphore: Arc<RwLock<u32>>,
}

struct HeapNode {
    prev_epoch: u64,
    tx: Option<oneshot::Sender<()>>,
}

impl HeapNode {
    fn new(prev_epoch: u64, tx: Option<oneshot::Sender<()>>) -> Self {
        Self { prev_epoch, tx }
    }

    fn notify_wait(&mut self) {
        if let Some(tx) = self.tx.take() {
            tx.send(()).ok();
        }
    }
}

impl Eq for HeapNode {}
impl PartialEq for HeapNode {
    fn eq(&self, other: &Self) -> bool {
        other.prev_epoch == self.prev_epoch
    }
}

impl PartialOrd for HeapNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        other.prev_epoch.partial_cmp(&self.prev_epoch)
    }
}

impl Ord for HeapNode
where
    Self: PartialOrd,
{
    fn cmp(&self, other: &HeapNode) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl<S> GlobalBarrierManager<S>
where
    S: MetaStore,
{
    /// Create a new [`crate::barrier::GlobalBarrierManager`].
    pub fn new(
        env: MetaSrvEnv<S>,
        cluster_manager: ClusterManagerRef<S>,
        catalog_manager: CatalogManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
        hummock_manager: HummockManagerRef<S>,
        metrics: Arc<MetaMetrics>,
    ) -> Self {
        let enable_recovery = env.opts.enable_recovery;
        let interval = env.opts.checkpoint_interval;
        tracing::info!(
            "Starting barrier manager with: interval={:?}, enable_recovery={}",
            interval,
            enable_recovery
        );

        Self {
            interval,
            enable_recovery,
            cluster_manager,
            catalog_manager,
            fragment_manager,
            scheduled_barriers: ScheduledBarriers::new(),
            hummock_manager,
            metrics,
            env,
            epoch_heap: Arc::new(RwLock::new(BinaryHeap::new())),
            wait_recovery: Arc::new(AtomicBool::new(false)),
            abort_command: Arc::new(RwLock::new(vec![])),
            semaphore: Arc::new(RwLock::new(0)),
        }
    }

    pub async fn start(
        barrier_manager: BarrierManagerRef<S>,
    ) -> (JoinHandle<()>, UnboundedSender<()>) {
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::unbounded_channel();
        let join_handle = tokio::spawn(async move {
            barrier_manager.run(shutdown_rx).await;
        });

        (join_handle, shutdown_tx)
    }

    /// Start an infinite loop to take scheduled barriers and send them.
    async fn run(&self, mut shutdown_rx: UnboundedReceiver<()>) {
        let env = self.env.clone();
        let mut unfinished = UnfinishedNotifiers::default();
        let mut state = BarrierManagerState::create(env.meta_store()).await;

        if self.enable_recovery {
            // handle init, here we simply trigger a recovery process to achieve the consistency. We
            // may need to avoid this when we have more state persisted in meta store.
            let new_epoch = Epoch::now();
            assert!(new_epoch > state.prev_epoch);
            state.prev_epoch = new_epoch;

            let (new_epoch, actors_to_finish, finished_create_mviews) =
                self.recovery(state.prev_epoch, None).await;
            unfinished.add(new_epoch.0, actors_to_finish, vec![]);
            for finished in finished_create_mviews {
                unfinished.finish_actors(finished.epoch, once(finished.actor_id));
            }
            state.prev_epoch = new_epoch;
            state.update(env.meta_store()).await.unwrap();
        }
        let mut last_epoch = state.prev_epoch.0;
        let mut min_interval = tokio::time::interval(self.interval);
        min_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let (barrier_send_tx, mut barrier_send_rx) = tokio::sync::mpsc::unbounded_channel();
        loop {
            tokio::select! {
                biased;
                // Shutdown
                _ = shutdown_rx.recv() => {
                    tracing::info!("Barrier manager is shutting down");
                    return;
                }
                result = barrier_send_rx.recv() =>{
                    match result{
                        Some(BarrierSendResult::Ok(new_epoch,actors_to_finish,notifiers,responses)) => {
                            unfinished.add(new_epoch.0, actors_to_finish, notifiers);

                            for finished in responses.into_iter().flat_map(|r| r.finished_create_mviews) {
                                unfinished.finish_actors(finished.epoch, once(finished.actor_id));
                            }

                        state.prev_epoch = new_epoch;

                        }
                        Some(BarrierSendResult::Err(command,e,isover)) => {
                            if self.enable_recovery {
                                // if not over , need wait all err barrier
                                self.wait_recovery.store(true,atomic::Ordering::SeqCst);
                                self.abort_command.write().await.push(command);
                                if isover{
                                    // If failed, enter recovery mode.
                                    let (new_epoch, actors_to_finish, finished_create_mviews) =
                                    self.recovery(state.prev_epoch, Some(self.abort_command.clone())).await;
                                    unfinished = UnfinishedNotifiers::default();
                                    unfinished.add(new_epoch.0, actors_to_finish, vec![]);
                                    for finished in finished_create_mviews {
                                        unfinished.finish_actors(finished.epoch, once(finished.actor_id));
                                    }

                                    state.prev_epoch = new_epoch;
                                    last_epoch = new_epoch.0;

                                    self.wait_recovery.store(false,atomic::Ordering::SeqCst);
                                    self.abort_command.write().await.clear();
                                }
                            } else {
                                panic!("failed to execute barrier: {:?}", e);
                            }
                        }
                        _=>{}
                    }
                    state.update(self.env.meta_store()).await.unwrap();
                    continue;
                }
                // there's barrier scheduled.
                _ = self.scheduled_barriers.wait_one() => {
                    if self.wait_recovery.load(atomic::Ordering::SeqCst){
                        continue;
                    }
                }
                // Wait for the minimal interval,
                _ = min_interval.tick() => {
                    if self.wait_recovery.load(atomic::Ordering::SeqCst){
                        continue;
                    }
                    if self.semaphore.read().await.gt(&0){
                        continue;
                    }
                }
            }
            // Get a barrier to send.
            let (command, notifiers) = self.scheduled_barriers.pop_or_default().await;

            match command {
                Command::Plain(..) => {}
                _ => {
                    let mut semaphore = self.semaphore.write().await;
                    *semaphore += 1;
                }
            }

            let info = self.resolve_actor_info(command.creating_table_id()).await;
            // When there's no actors exist in the cluster, we don't need to send the barrier. This
            // is an advance optimization. Besides if another barrier comes immediately,
            // it may send a same epoch and fail the epoch check.
            if info.nothing_to_do() {
                let mut notifiers = notifiers;
                notifiers.iter_mut().for_each(Notifier::notify_to_send);
                notifiers.iter_mut().for_each(Notifier::notify_collected);
                continue;
            }
            let new_epoch = Epoch::now();
            let prev_epoch = Epoch::from(last_epoch);
            last_epoch = new_epoch.0;
            // debug!("new_epoch{:?},prev_epoch{:?},command:{:?}",new_epoch,prev_epoch,command);
            assert!(
                new_epoch > prev_epoch,
                "new{:?},prev{:?}",
                new_epoch,
                prev_epoch
            );

            let env = self.env.clone();
            let fragment_manager = self.fragment_manager.clone();
            let metrics = self.metrics.clone();
            let barrier_send_tx_clone = barrier_send_tx.clone();
            let heap = self.epoch_heap.clone();
            let hummock_manager = self.hummock_manager.clone();
            let wait_recovery = self.wait_recovery.clone();
            let semaphore = self.semaphore.clone();

            tokio::spawn(async move {
                let (wait_tx, wait_rx) = oneshot::channel();
                heap.write()
                    .await
                    .push(HeapNode::new(prev_epoch.0, Some(wait_tx)));

                let env = env.clone();
                let command_ctx = CommandContext::new(
                    fragment_manager.clone(),
                    env.stream_clients_ref(),
                    &info,
                    &prev_epoch,
                    &new_epoch,
                    command.clone(),
                );
                let mut notifiers = notifiers;
                notifiers.iter_mut().for_each(Notifier::notify_to_send);
                let result = GlobalBarrierManager::run_inner(
                    env.clone(),
                    metrics,
                    &command_ctx,
                    heap.clone(),
                    hummock_manager,
                    wait_recovery,
                    wait_rx,
                )
                .await;

                // debug!("over,new_epoch{:?}",new_epoch);
                match command {
                    Command::Plain(..) => {}
                    _ => {
                        let mut semaphore = semaphore.write().await;
                        *semaphore -= 1;
                    }
                }
                match result {
                    Ok(responses) => {
                        // Notify about collected first.
                        notifiers.iter_mut().for_each(Notifier::notify_collected);
                        // Then try to finish the barrier for Create MVs.
                        let actors_to_finish = command_ctx.actors_to_finish();
                        barrier_send_tx_clone
                            .send(BarrierSendResult::Ok(
                                new_epoch,
                                actors_to_finish,
                                notifiers,
                                responses,
                            ))
                            .unwrap();
                    }
                    Err(e) => {
                        notifiers
                            .into_iter()
                            .for_each(|notifier| notifier.notify_collection_failed(e.clone()));
                        let mut is_over = false;
                        if heap.read().await.len() == 1 {
                            is_over = true;
                        }
                        barrier_send_tx_clone
                            .send(BarrierSendResult::Err(command, e, is_over))
                            .unwrap();
                    }
                }
                heap.write().await.pop();
                if let Some(mut next_heap_node) = heap.write().await.peek_mut() {
                    next_heap_node.notify_wait();
                }
            });
        }
    }

    /// Running a scheduled command.
    async fn run_inner<'a>(
        env: MetaSrvEnv<S>,
        metrics: Arc<MetaMetrics>,
        command_context: &CommandContext<'a, S>,
        heap: Arc<RwLock<BinaryHeap<HeapNode>>>,
        hummock_manager: HummockManagerRef<S>,
        wait_recovery: Arc<AtomicBool>,
        wait_rx: oneshot::Receiver<()>,
    ) -> Result<Vec<InjectBarrierResponse>> {
        let timer = metrics.barrier_latency.start_timer();

        // Wait for all barriers collected
        let result = GlobalBarrierManager::inject_barrier(env, command_context).await;
        // Commit this epoch to Hummock
        if command_context.prev_epoch.0 != INVALID_EPOCH {
            let heap_read = heap.read().await;
            if let Some(heap_node) = heap_read.peek() {
                let prev_epoch = heap_node.prev_epoch;
                if command_context.prev_epoch.0 != prev_epoch {
                    drop(heap_read);
                    wait_rx.await.unwrap();
                }
            }
            if wait_recovery.load(atomic::Ordering::SeqCst) {
                hummock_manager
                    .abort_epoch(command_context.prev_epoch.0)
                    .await?;
                result?;
                return Err(RwError::from(ErrorCode::InternalError("err".to_string())));
            }
            match result {
                Ok(_) => {
                    // We must ensure all epochs are committed in ascending order, because
                    // the storage engine will query from new to old in the order in which
                    // the L0 layer files are generated. see https://github.com/singularity-data/risingwave/issues/1251
                    hummock_manager
                        .commit_epoch(command_context.prev_epoch.0)
                        .await?;
                }
                Err(_) => {
                    hummock_manager
                        .abort_epoch(command_context.prev_epoch.0)
                        .await?;
                }
            };
        }
        let responses = result?;

        timer.observe_duration();
        command_context.post_collect().await?; // do some post stuffs
        Ok(responses)
    }

    /// Inject barrier to all computer nodes.
    async fn inject_barrier<'a>(
        env: MetaSrvEnv<S>,
        command_context: &CommandContext<'a, S>,
    ) -> Result<Vec<InjectBarrierResponse>> {
        let mutation = command_context.to_mutation().await?;
        let info = command_context.info;

        let collect_futures = info.node_map.iter().filter_map(|(node_id, node)| {
            let actor_ids_to_send = info.actor_ids_to_send(node_id).collect_vec();
            let actor_ids_to_collect = info.actor_ids_to_collect(node_id).collect_vec();

            if actor_ids_to_collect.is_empty() {
                // No need to send or collect barrier for this node.
                assert!(actor_ids_to_send.is_empty());
                None
            } else {
                let mutation = mutation.clone();
                let request_id = Uuid::new_v4().to_string();
                let barrier = Barrier {
                    epoch: Some(risingwave_pb::data::Epoch {
                        curr: command_context.curr_epoch.0,
                        prev: command_context.prev_epoch.0,
                    }),
                    mutation: Some(mutation),
                    // TODO(chi): add distributed tracing
                    span: vec![],
                };
                // debug!("inject_barrier to cn {:?}", barrier);
                let env = env.clone();
                async move {
                    let mut client = env.stream_clients().get(node).await?;

                    let request = InjectBarrierRequest {
                        request_id,
                        barrier: Some(barrier),
                        actor_ids_to_send,
                        actor_ids_to_collect,
                    };
                    tracing::trace!(
                        target: "events::meta::barrier::inject_barrier",
                        "inject barrier request: {:?}", request
                    );

                    // This RPC returns only if this worker node has collected this barrier.
                    client
                        .inject_barrier(request)
                        .await
                        .map(tonic::Response::<_>::into_inner)
                        .to_rw_result()
                }
                .into()
            }
        });

        try_join_all(collect_futures).await
    }

    /// Resolve actor information from cluster and fragment manager.
    async fn resolve_actor_info(&self, creating_table_id: Option<TableId>) -> BarrierActorInfo {
        let all_nodes = self
            .cluster_manager
            .list_worker_node(WorkerType::ComputeNode, Some(Running))
            .await;
        let all_actor_infos = self
            .fragment_manager
            .load_all_actors(creating_table_id)
            .await;
        BarrierActorInfo::resolve(all_nodes, all_actor_infos)
    }

    async fn do_schedule(&self, command: Command, notifier: Notifier) -> Result<()> {
        self.scheduled_barriers
            .push((command, once(notifier).collect()))
            .await;
        Ok(())
    }

    /// Schedule a command and return immediately.
    #[allow(dead_code)]
    pub async fn schedule_command(&self, command: Command) -> Result<()> {
        self.do_schedule(command, Default::default()).await
    }

    /// Schedule a command and return when its corresponding barrier is about to sent.
    #[allow(dead_code)]
    pub async fn issue_command(&self, command: Command) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.do_schedule(
            command,
            Notifier {
                to_send: Some(tx),
                ..Default::default()
            },
        )
        .await?;
        rx.await.unwrap();

        Ok(())
    }

    /// Run a command and return when it's completely finished.
    pub async fn run_command(&self, command: Command) -> Result<()> {
        let (collect_tx, collect_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();

        let is_create_mv = matches!(command, Command::CreateMaterializedView { .. });

        self.do_schedule(
            command,
            Notifier {
                collected: Some(collect_tx),
                finished: Some(finish_tx),
                ..Default::default()
            },
        )
        .await?;

        collect_rx.await.unwrap()?; // Throw the error if it occurs when collecting this barrier.

        // TODO: refactor this
        if is_create_mv {
            // The snapshot ingestion may last for several epochs, we should pin the epoch here.
            // TODO: this should be done in `post_collect`
            let snapshot = self
                .hummock_manager
                .pin_snapshot(META_NODE_ID, HummockEpoch::MAX)
                .await?;
            finish_rx.await.unwrap(); // Wait for this command to be finished.
            self.hummock_manager
                .unpin_snapshot(META_NODE_ID, [snapshot])
                .await?;
        } else {
            finish_rx.await.unwrap(); // Wait for this command to be finished.
        }

        Ok(())
    }

    /// Wait for the next barrier to collect. Note that the barrier flowing in our stream graph is
    /// ignored, if exists.
    pub async fn wait_for_next_barrier_to_collect(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let notifier = Notifier {
            collected: Some(tx),
            ..Default::default()
        };
        self.scheduled_barriers
            .attach_notifiers(once(notifier))
            .await;
        rx.await.unwrap()
    }
}

pub type BarrierManagerRef<S> = Arc<GlobalBarrierManager<S>>;
