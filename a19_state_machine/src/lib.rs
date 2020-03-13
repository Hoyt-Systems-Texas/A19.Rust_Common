use a19_concurrent::queue::mpsc_queue::{MpscQueueReceive, MpscQueueWrap};
use a19_concurrent::queue::skip_queue::{create_skip_queue, SkipQueueReader};
use async_trait::async_trait;
use core::pin::Pin;
use futures::channel::oneshot::{self, channel};
use futures::future::Future;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::hash::Hash;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::task;

const STATE_INACTIVE: u32 = 0;
const STATE_RUNNING: u32 = 1;

type StateMachineType<KEY, STATE, MODEL, MSG, RESULT> =
    ContextOption<KEY, STATE, MODEL, MSG, RESULT>;
type MsgFuture<RESULT> = Option<oneshot::Sender<StateMachineResult<RESULT>>>;

pub enum StateMachineResult<RESULT> {
    Full,
    Ran(RESULT),
}

pub struct LoadInfo<KEY: Hash + Eq, STATE: Hash + Eq + Send, MODEL: Send> {
    pub id: KEY,
    pub last_transition_id: u64,
    pub current_state: STATE,
    pub model: MODEL,
}

/// Represents an internal message format.  We need the ability to add a future.
enum InternalMessage<KEY: Hash + Eq, STATE: Hash + Eq + Send, MODEL: Send, MSG, RESULT> {
    UserMessage {
        key: KEY,
        msg: MSG,
        future: MsgFuture<RESULT>,
    },
    ModelLoaded(LoadInfo<KEY, STATE, MODEL>),
}

enum CtxMessage<MSG, RESULT> {
    UserMessage { msg: MSG, future: MsgFuture<RESULT> },
}

enum StateMachineEvt<KEY: Hash + Eq, MSG> {
    SendMessage { key: KEY, msg: MSG },
}

struct ContextContainer<KEY: Hash + Eq, STATE: Hash + Eq + Send, MODEL, MSG, RESULT> {
    cell: UnsafeCell<Context<KEY, STATE, MODEL, MSG, RESULT>>,
}

unsafe impl<KEY: Hash + Eq, STATE: Hash + Eq + Send, MODEL, MSG, RESULT> Sync
    for ContextContainer<KEY, STATE, MODEL, MSG, RESULT>
{
}
unsafe impl<KEY: Hash + Eq, STATE: Hash + Eq + Send, MODEL, MSG, RESULT> Send
    for ContextContainer<KEY, STATE, MODEL, MSG, RESULT>
{
}

struct StateMachineReceiver<
    EVT,
    KEY: Hash + Eq,
    STATE: Hash + Eq + Send,
    MODEL: Send,
    MSG,
    RESULT,
    F,
    FT: Future<Output = LoadInfo<KEY, STATE, MODEL>> + Send + 'static,
> where
    F: Fn(KEY) -> FT,
{
    state: Arc<AtomicU32>,
    context_store: HashMap<KEY, StateMachineType<KEY, STATE, MODEL, MSG, RESULT>>,
    state_store: HashMap<STATE, Box<dyn StatePersisted<EVT, KEY, STATE, MODEL, MSG, RESULT>>>,
    queue_reader: MpscQueueReceive<InternalMessage<KEY, STATE, MODEL, MSG, RESULT>>,
    load: Arc<F>,
}

pub struct StateMachineSender<KEY: Hash + Eq, STATE: Hash + Eq + Send, MODEL: Send, MSG, RESULT> {
    state: Arc<AtomicU32>,
    queue_writer: Arc<MpscQueueWrap<InternalMessage<KEY, STATE, MODEL, MSG, RESULT>>>,
    receiver_thread: JoinHandle<u32>,
}

impl<KEY: Hash + Eq, STATE: Hash + Eq + Send, MODEL: Send, MSG, RESULT> StateMachineSender<KEY, STATE, MODEL, MSG, RESULT> {
    /// Used to send a message to a state machine.
    /// # Arguments
    /// `key` - The key for the state machine.
    /// `msg` - The message for the state machine.
    pub fn send(&self, key: KEY, msg: MSG) -> oneshot::Receiver<StateMachineResult<RESULT>> {
        let (s, r) = channel();
        let msg = InternalMessage::UserMessage::<KEY, STATE, MODEL, MSG, RESULT> {
            key,
            msg,
            future: Some(s),
        };
        r
    }
}

/// Used to create a state machine.  Currently for the main loop uses the single writer principal.
/// Would require finding a high speed hashmap to not do it this way.
pub fn create_state_machine<
    EVT,
    KEY: Hash + Eq + Clone + Send + 'static,
    STATE: 'static + Hash + Eq + Send,
    MODEL: 'static + Send,
    MSG: 'static,
    RESULT: 'static,
    F,
    FT,
>(
    load_func: F,
    queue_size: usize,
) -> StateMachineSender<KEY, STATE, MODEL, MSG, RESULT>
where
    F: Fn(KEY) -> FT + Send + Sync + 'static,
    FT: Future<Output = LoadInfo<KEY, STATE, MODEL>> + Send + 'static,
{
    let (writer, reader) = MpscQueueWrap::new(queue_size);
    let writer = Arc::new(writer);
    let state = Arc::new(AtomicU32::new(1));
    let receiver_state = state.clone();
    let main_thread_writer = writer.clone();
    let join_handle = thread::spawn(move || {
        let context_store = HashMap::with_capacity(100);
        let mut receiver = StateMachineReceiver::<EVT, KEY, STATE, MODEL, MSG, RESULT, F, FT> {
            context_store,
            load: Arc::new(load_func),
            queue_reader: reader,
            state: receiver_state,
            state_store: HashMap::with_capacity(20),
        };
        let wait_time = Duration::from_millis(1);
        loop {
            if receiver.state.load(Ordering::Relaxed) == 0 {
                break;
            } else {
                if let Some(m) = receiver.queue_reader.poll() {
                    match m {
                        InternalMessage::UserMessage { key, msg, future } => {
                            if let Some(i) = receiver.context_store.get_mut(&key) {
                                let user_message = CtxMessage::UserMessage { msg, future };
                                match i {
                                    ContextOption::Loaded(ctx_v) => {
                                        let ctx = unsafe { &mut *ctx_v.cell.get() };
                                        let ctx_v = ctx_v.clone();
                                        ctx.int_add_message(user_message);
                                        tokio::spawn(async move {
                                            let ctx = unsafe { &mut *ctx_v.cell.get() };
                                            ctx.run();
                                        });
                                    }
                                    ContextOption::PendingLoad(ctx) => {
                                        ctx.int_add_message(user_message);
                                    }
                                }
                            } else {
                                let (writer, reader) =
                                    create_skip_queue::<CtxMessage<MSG, RESULT>>(queue_size);
                                let pending = PendingInfo {
                                    id: key.clone(),
                                    queue_reader: reader,
                                    queue_writer: writer,
                                };
                                let ctx =ContextOption::PendingLoad(pending);
                                receiver.context_store.insert(key.clone(), ctx);
                                let load = receiver.load.clone();
                                let new_key = key.clone();
                                tokio::spawn(async move {
                                    let result = (load)(new_key).await;
                                });
                            }
                        },
                        InternalMessage::ModelLoaded(model) => {
                            if let Some(current_ctx) = receiver.context_store.remove(&model.id) {
                                match current_ctx {
                                    ContextOption::PendingLoad(p) => {
                                        let new_context = Arc::new(
                                            ContextContainer {
                                                cell: UnsafeCell::new(
                                                Context {
                                                current_state: model.current_state,
                                                id: model.id.clone(),
                                                last_transition_id: model.last_transition_id,
                                                model: model.model,
                                                queue_reader: p.queue_reader,
                                                queue_writer: p.queue_writer,
                                                state_machine_state: AtomicU32::new(1),
                                                version: AtomicU64::new(0),
                                            }           
                                                )
                                            }
                                        );
                                        receiver.context_store.insert(
                                            model.id.clone(),
                                            ContextOption::Loaded(new_context.clone()));
                                        // Process any pending messages.
                                        tokio::spawn(async move {
                                            let ctx = unsafe {&mut *new_context.cell.get()};
                                            ctx.run();
                                        });
                                    }
                                    _ => {
                                        
                                    }
                                }
                            } else {
                                // TODO figure out where to writer errors.
                            }
                        }
                    }
                } else {
                    thread::sleep(wait_time);
                }
            }
        }
        1
    });
    StateMachineSender {
        queue_writer: writer.clone(),
        receiver_thread: join_handle,
        state: state.clone(),
    }
}

#[async_trait]
pub trait StatePersisted<EVT, KEY: Hash + Eq, STATE: Hash + Eq + Send, MODEL, MSG, RESULT> {
    /// The state this node if for.
    fn state(&self) -> STATE where Self:Sized;

    /// Called when an event is received when we are in that state.
    /// # Arguments
    /// `event` - The event that has been received.
    async fn events<F>(&self, event: &EVT) -> EventActionResult<STATE> where Self:Sized;

    /// The entry for the state.
    /// # Arguments
    /// `evt` - The event we are enterying.
    /// `ctx` - The mutable context.
    /// `msg` - The message we are processing.
    async fn entry(&self, evt: &EVT, ctx: &mut Context<KEY, STATE, MODEL, MSG, RESULT>, msg: &MSG) where Self:Sized;

    /// Called when we exit a state.
    /// # Arguments
    /// `evt` - The event we are processing.
    /// `ctx` - The mutable context for the state transition.
    /// `msg` - The message we are processing.
    async fn exit(&self, evt: &EVT, ctx: &mut Context<KEY, STATE, MODEL, MSG, RESULT>, msg: &MSG) where Self: Sized;
}

enum ContextOption<KEY: Hash + Eq, STATE:Hash + Eq + Send, MODEL, MSG, RESULT> {
    PendingLoad(PendingInfo<KEY, MSG, RESULT>),
    Loaded(Arc<ContextContainer<KEY, STATE, MODEL, MSG, RESULT>>),
}

pub struct Context<KEY, STATE, MODEL, MSG, RESULT> {
    id: KEY,
    current_state: STATE,
    model: MODEL,
    last_transition_id: u64,
    version: AtomicU64,
    queue_reader: SkipQueueReader<CtxMessage<MSG, RESULT>>,
    queue_writer: MpscQueueWrap<CtxMessage<MSG, RESULT>>,
    state_machine_state: AtomicU32,
}

struct PendingInfo<KEY, MSG, RESULT> {
    id: KEY,
    queue_reader: SkipQueueReader<CtxMessage<MSG, RESULT>>,
    queue_writer: MpscQueueWrap<CtxMessage<MSG, RESULT>>,
}

impl<KEY, MSG, RESULT> PendingInfo<KEY, MSG, RESULT> {
    fn int_add_message(&self, msg: CtxMessage<MSG, RESULT>) {
        self.queue_writer.offer(msg);
    }
}

/// Represents the thread safe client for the state machine.
pub struct StateMachineClient<MSG> {
    queue: MpscQueueWrap<MSG>,
}

impl<KEY: Hash + Eq, STATE, MODEL, MSG, RESULT> Context<KEY, STATE, MODEL, MSG, RESULT> {
    pub fn new(
        id: KEY,
        current_state: STATE,
        model: MODEL,
        last_transition_id: u64,
        queue_size: usize,
    ) -> (Self) {
        let (queue_writer, queue_reader) = create_skip_queue(queue_size);
        let context = Context {
            id,
            current_state,
            model,
            last_transition_id,
            version: AtomicU64::new(0),
            queue_reader,
            queue_writer,
            state_machine_state: AtomicU32::new(STATE_INACTIVE),
        };
        context
    }

    pub fn id(&self) -> &KEY {
        &self.id
    }

    /// Sets the state of the model.
    pub fn set_state(&mut self, state: STATE) {
        self.current_state = state;
    }

    /// Gets the model for edditing.
    pub fn model<'a>(&'a mut self) -> &'a mut MODEL {
        &mut self.model
    }

    /// Gets the last transition id.
    pub fn last_transition_id(&self) -> &u64 {
        &self.last_transition_id
    }

    pub fn set_last_transition_id(&mut self, id: u64) {
        self.last_transition_id = id;
    }

    /// Gets the current state of the context.
    pub fn state(&self) -> &STATE {
        &self.current_state
    }

    /// The current version on the context.
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Called when the context is acquired for an operation.
    fn acquired(&self) {
        self.version.fetch_add(1, Ordering::AcqRel);
    }

    /// Called when the context is completed after an operation.
    fn completed(&self) {
        self.version.fetch_add(1, Ordering::AcqRel);
    }

    /// Mean to be run in a thread pull and can run asynchronously.
    pub fn run(&mut self) {
        if self.state_machine_state.compare_and_swap(
            STATE_INACTIVE,
            STATE_RUNNING,
            Ordering::AcqRel,
        ) == STATE_INACTIVE
        {
            self.acquired();
            loop {
                if let Some(m) = self.queue_reader.poll() {
                    // Get the state
                } else {
                    break
                }
            }
            self.completed();
            self.state_machine_state
                .store(STATE_INACTIVE, Ordering::Release)
        }
    }

    /// Adds a message to the queue.
    pub fn add_message(&self, msg: MSG, future: MsgFuture<RESULT>) -> bool {
        self.queue_writer
            .offer(CtxMessage::UserMessage { future, msg })
    }

    fn int_add_message(&self, msg: CtxMessage<MSG, RESULT>) {
        self.queue_writer.offer(msg);
    }
}

unsafe impl<KEY: Hash + Eq, STATE, MODEL, MSG, RESULT> Sync
    for Context<KEY, STATE, MODEL, MSG, RESULT>
{
}
unsafe impl<KEY: Hash + Eq, STATE, MODEL, MSG, RESULT> Send
    for Context<KEY, STATE, MODEL, MSG, RESULT>
{
}

#[async_trait]
pub trait Action<KEY: Hash + Eq, STATE, MODEL, MSG, RESULT> {
    /// Used to run the specified action with the ctx and messag.
    /// # Arguments
    /// `ctx` - The context for the state machine.
    /// `msg` - The message for the state machine.
    async fn run(&self, ctx: &mut Context<KEY, STATE, MODEL, MSG, RESULT>, msg: &MSG) -> RESULT;
}

pub enum EventActionResult<STATE: Hash + Eq> {
    GoToState { state: STATE},
    DidAction,
    Ignore,
    Defer,
}

#[derive(Debug)]
struct EventActionNode<KEY: Hash + Eq, MSG, EVENT> {
    id: KEY,
    msg: MSG,
    event: EVENT,
}

#[cfg(test)]
mod tests {}
