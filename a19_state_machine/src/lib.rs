use a19_concurrent::queue::mpsc_queue::{MpscQueueReceive, MpscQueueWrap};
use a19_concurrent::queue::skip_queue::{create_skip_queue, SkipQueueReader};
use a19_concurrent::timeout::consistent::TimeoutFixed;
use a19_core::current_time_secs;
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
use tokio::runtime::Handle;
use tokio::task;

const STATE_INACTIVE: u32 = 0;
const STATE_RUNNING: u32 = 1;

type StateMachineType<KEY, STATE, MODEL, MSG, RESULT> =
    ContextOption<KEY, STATE, MODEL, MSG, RESULT>;
type MsgFuture<RESULT, STATE> = Option<oneshot::Sender<StateMachineResult<RESULT, STATE>>>;

pub enum StateMachineResult<RESULT, STATE> {
    Full,
    Ran(RESULT),
    ChangedState(STATE),
    Ignored,
}

pub struct LoadInfo<KEY: Hash + Eq, STATE: Hash + Eq + Send + Clone, MODEL: Send> {
    pub id: KEY,
    pub last_transition_id: u64,
    pub current_state: STATE,
    pub model: MODEL,
}

/// Represents an internal message format.  We need the ability to add a future.
enum InternalMessage<
    KEY: Hash + Eq,
    STATE: Hash + Eq + Send + Clone,
    MODEL: Send,
    MSG: Send,
    RESULT,
> {
    UserMessage {
        key: KEY,
        msg: MSG,
        future: MsgFuture<RESULT, STATE>,
    },
    ModelLoaded(LoadInfo<KEY, STATE, MODEL>),
}

enum CtxMessage<MSG, RESULT, STATE> {
    UserMessage {
        msg: MSG,
        future: MsgFuture<RESULT, STATE>,
    },
}

enum StateMachineEvt<KEY: Hash + Eq, MSG> {
    SendMessage { key: KEY, msg: MSG },
}

pub struct ContextContainer<
    KEY: Hash + Eq,
    STATE: Hash + Eq + Send + Clone,
    MODEL,
    MSG: Send,
    RESULT,
> {
    cell: UnsafeCell<Context<KEY, STATE, MODEL, MSG, RESULT>>,
}

impl<KEY: Hash + Eq, STATE: Hash + Eq + Send + Clone, MODEL, MSG: Send, RESULT>
    ContextContainer<KEY, STATE, MODEL, MSG, RESULT>
{
    fn ctx<'a>(&self) -> &'a mut Context<KEY, STATE, MODEL, MSG, RESULT> {
        unsafe { &mut *self.cell.get() }
    }

    pub fn id(&self) -> &KEY {
        &self.ctx().id
    }

    /// Sets the state of the model.
    pub fn set_state(&self, state: STATE) {
        self.ctx().current_state = state;
    }

    /// Gets the model for edditing.
    pub fn model<'a>(&'a mut self) -> &'a mut MODEL {
        &mut self.ctx().model
    }

    /// Gets the last transition id.
    pub fn last_transition_id(&self) -> &u64 {
        &self.ctx().last_transition_id
    }

    pub fn set_last_transition_id(&mut self, id: u64) {
        self.ctx().last_transition_id = id;
    }

    /// Gets the current state of the context.
    pub fn state(&self) -> &STATE {
        &self.ctx().current_state
    }

    /// The current version on the context.
    pub fn version(&self) -> u64 {
        self.ctx().version.load(Ordering::Acquire)
    }

    /// Called when the context is acquired for an operation.
    fn acquired(&self) {
        self.ctx().version.fetch_add(1, Ordering::AcqRel);
    }

    /// Called when the context is completed after an operation.
    fn completed(&self) {
        self.ctx().version.fetch_add(1, Ordering::AcqRel);
    }

    /// Adds a message to the queue.
    pub fn add_message(&self, msg: MSG, future: MsgFuture<RESULT, STATE>) -> bool {
        self.ctx()
            .queue_writer
            .offer(CtxMessage::UserMessage { future, msg })
    }

    fn int_add_message(&self, msg: CtxMessage<MSG, RESULT, STATE>) {
        self.ctx().queue_writer.offer(msg);
    }

    fn skip_message(&self, msg: MSG, future: MsgFuture<RESULT, STATE>) -> bool {
        self.ctx()
            .queue_reader
            .skip(CtxMessage::UserMessage { msg, future });
        true
    }

    fn start(&self) -> bool {
        self.ctx().state_machine_state.compare_and_swap(
            STATE_INACTIVE,
            STATE_RUNNING,
            Ordering::AcqRel,
        ) == STATE_INACTIVE
    }

    fn finish(&self) {
        self.ctx()
            .state_machine_state
            .store(STATE_INACTIVE, Ordering::Release)
    }

    fn poll(&self) -> Option<CtxMessage<MSG, RESULT, STATE>> {
        self.ctx().queue_reader.poll()
    }

    fn peek(&self) -> Option<&CtxMessage<MSG, RESULT, STATE>> {
        self.ctx().queue_reader.peek()
    }
}

unsafe impl<KEY: Hash + Eq, STATE: Hash + Eq + Send + Clone, MODEL, MSG: Send, RESULT> Sync
    for ContextContainer<KEY, STATE, MODEL, MSG, RESULT>
{
}
unsafe impl<KEY: Hash + Eq, STATE: Hash + Eq + Send + Clone, MODEL, MSG: Send, RESULT> Send
    for ContextContainer<KEY, STATE, MODEL, MSG, RESULT>
{
}

struct StateMachineReceiver<
    KEY: Hash + Eq,
    STATE: Hash + Eq + Send + Clone,
    MODEL: Send,
    MSG: Send,
    RESULT,
    F,
    FT: Future<Output = LoadInfo<KEY, STATE, MODEL>> + Send + 'static,
> where
    F: Fn(KEY) -> FT,
{
    state: Arc<AtomicU32>,
    context_store: HashMap<KEY, StateMachineType<KEY, STATE, MODEL, MSG, RESULT>>,
    state_store: Arc<HashMap<STATE, Box<dyn StatePersisted<KEY, STATE, MODEL, MSG, RESULT>>>>,
    queue_reader: MpscQueueReceive<InternalMessage<KEY, STATE, MODEL, MSG, RESULT>>,
    load: Arc<F>,
}

pub struct StateMachineSender<
    KEY: Hash + Eq,
    STATE: Hash + Eq + Send + Clone,
    MODEL: Send,
    MSG: Send,
    RESULT,
> {
    state: Arc<AtomicU32>,
    queue_writer: Arc<MpscQueueWrap<InternalMessage<KEY, STATE, MODEL, MSG, RESULT>>>,
    receiver_thread: JoinHandle<u32>,
}

impl<KEY: Hash + Eq, STATE: Hash + Eq + Send + Clone, MODEL: Send, MSG: Send, RESULT>
    StateMachineSender<KEY, STATE, MODEL, MSG, RESULT>
{
    /// Used to send a message to a state machine.
    /// # Arguments
    /// `key` - The key for the state machine.
    /// `msg` - The message for the state machine.
    pub fn send(&self, key: KEY, msg: MSG) -> oneshot::Receiver<StateMachineResult<RESULT, STATE>> {
        let (s, r) = channel();
        let msg = InternalMessage::UserMessage::<KEY, STATE, MODEL, MSG, RESULT> {
            key,
            msg,
            future: Some(s),
        };
        if self.queue_writer.offer(msg) {
            // Success
            r
        } else {
            let (s, r) = channel();
            s.send(StateMachineResult::Ignored);
            r
        }
    }
}

fn timeout_ctx() -> u64 {
    current_time_secs() + 3000
}

/// Used to create a state machine.  Currently for the main loop uses the single writer principal.
/// Would require finding a high speed hashmap to not do it this way.
pub fn create_state_machine<
    KEY: Hash + Eq + Clone + Send + 'static,
    STATE: 'static + Hash + Eq + Send + Sync + Clone,
    MODEL: 'static + Send + Sync,
    MSG: 'static + Send + Sync,
    RESULT: 'static + Send + Sync,
    F,
    FT,
>(
    load_func: F,
    states: &mut Vec<Box<dyn StatePersisted<KEY, STATE, MODEL, MSG, RESULT>>>,
    queue_size: usize,
    runtime: Handle,
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
    let mut state_store = HashMap::with_capacity(100);
    loop {
        if let Some(state) = states.pop() {
            state_store.insert(state.state(), state);
        } else {
            break;
        }
    }
    let main_queue_writer = writer.clone();
    let join_handle = thread::spawn(move || {
        let context_store = HashMap::with_capacity(100);
        let mut timeout = TimeoutFixed::<KEY>::with_capacity(5000);
        let mut receiver = StateMachineReceiver::<KEY, STATE, MODEL, MSG, RESULT, F, FT> {
            context_store,
            load: Arc::new(load_func),
            queue_reader: reader,
            state: receiver_state,
            state_store: Arc::new(state_store),
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
                                        let map = receiver.state_store.clone();
                                        ctx.int_add_message(user_message);
                                        runtime.spawn(async move {
                                            run(ctx_v, map).await;
                                        });
                                    }
                                    ContextOption::PendingLoad(ctx) => {
                                        ctx.int_add_message(user_message);
                                    }
                                }
                            } else {
                                let (writer, reader) =
                                    create_skip_queue::<CtxMessage<MSG, RESULT, STATE>>(queue_size);
                                let pending = PendingInfo {
                                    id: key.clone(),
                                    queue_reader: reader,
                                    queue_writer: writer,
                                };
                                let user_message = CtxMessage::UserMessage { msg, future };
                                pending.queue_writer.offer(user_message);
                                let ctx = ContextOption::PendingLoad(pending);
                                receiver.context_store.insert(key.clone(), ctx);
                                let load = receiver.load.clone();
                                let new_key = key.clone();
                                let main_queue_writer = main_queue_writer.clone();
                                runtime.spawn(async move {
                                    let result = (load)(new_key).await;
                                    main_queue_writer.offer(InternalMessage::ModelLoaded(result))
                                });
                            }
                        }
                        InternalMessage::ModelLoaded(model) => {
                            if let Some(current_ctx) = receiver.context_store.remove(&model.id) {
                                match current_ctx {
                                    ContextOption::PendingLoad(p) => {
                                        let new_context = Arc::new(ContextContainer {
                                            cell: UnsafeCell::new(Context::<
                                                KEY,
                                                STATE,
                                                MODEL,
                                                MSG,
                                                RESULT,
                                            > {
                                                current_state: model.current_state,
                                                id: model.id.clone(),
                                                last_transition_id: model.last_transition_id,
                                                model: model.model,
                                                queue_reader: p.queue_reader,
                                                queue_writer: p.queue_writer,
                                                state_machine_state: AtomicU32::new(STATE_INACTIVE),
                                                version: AtomicU64::new(0),
                                            }),
                                        });
                                        receiver.context_store.insert(
                                            model.id.clone(),
                                            ContextOption::Loaded(new_context.clone()),
                                        );
                                        timeout.add(new_context.id().clone(), timeout_ctx(), new_context.version());
                                        // Process any pending messages.
                                        let states = receiver.state_store.clone();
                                        runtime.spawn(async move {
                                            run(new_context, states).await;
                                        });
                                    }
                                    _ => {}
                                }
                            } else {
                                // TODO figure out where to writer errors.
                            }
                        }
                    }
                } else {
                    let current_time = current_time_secs();
                    // Going to do the cleanup in the main thread to prevent having to handle complex race conditions and needing a concurrent hashmap.
                    loop {
                        if let Some(top) = timeout.pop_expired(&current_time) {
                            if let Some(ctx) = receiver.context_store.get(&top.key) {
                                match ctx {
                                    ContextOption::Loaded(ctx) => {
                                        if ctx.version() == top.version && ctx.version() % 2 == 0 {
                                            receiver.context_store.remove(&top.key);
                                        } else {
                                            timeout.add(top.key.clone(), timeout_ctx(), ctx.version());
                                        }
                                    }
                                    _ => {
                                        // Just remove it.  Something went wrong and we should just delete it.
                                        receiver.context_store.remove(&top.key);
                                    }
                                }
                            }
                        } else {
                            break
                        }
                    }
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
pub trait StatePersisted<KEY: Hash + Eq, STATE: Hash + Eq + Send + Clone, MODEL, MSG: Send, RESULT>:
    Send + Sync
{
    /// The state this node if for.
    fn state(&self) -> STATE;

    /// Called when an event is received when we are in that state.
    /// # Arguments
    /// `event` - The event that has been received.
    async fn handle_event(
        &self,
        event: &MSG,
        ctx: Arc<ContextContainer<KEY, STATE, MODEL, MSG, RESULT>>,
    ) -> EventActionResult<STATE, RESULT>;

    /// The entry for the state.
    /// # Arguments
    /// `evt` - The event we are enterying.
    /// `ctx` - The mutable context.
    /// `msg` - The message we are processing.
    async fn entry(&self, evt: &MSG, ctx: Arc<ContextContainer<KEY, STATE, MODEL, MSG, RESULT>>);

    /// Called when we exit a state.
    /// # Arguments
    /// `evt` - The event we are processing.
    /// `ctx` - The mutable context for the state transition.
    /// `msg` - The message we are processing.
    async fn exit(&self, evt: &MSG, ctx: Arc<ContextContainer<KEY, STATE, MODEL, MSG, RESULT>>);
}

enum ContextOption<KEY: Hash + Eq, STATE: Hash + Eq + Send + Clone, MODEL, MSG: Send, RESULT> {
    PendingLoad(PendingInfo<KEY, STATE, MSG, RESULT>),
    Loaded(Arc<ContextContainer<KEY, STATE, MODEL, MSG, RESULT>>),
}

pub struct Context<KEY, STATE, MODEL, MSG: Send, RESULT> {
    id: KEY,
    current_state: STATE,
    model: MODEL,
    last_transition_id: u64,
    version: AtomicU64,
    queue_reader: SkipQueueReader<CtxMessage<MSG, RESULT, STATE>>,
    queue_writer: MpscQueueWrap<CtxMessage<MSG, RESULT, STATE>>,
    state_machine_state: AtomicU32,
}

struct PendingInfo<KEY, STATE, MSG, RESULT> {
    id: KEY,
    queue_reader: SkipQueueReader<CtxMessage<MSG, RESULT, STATE>>,
    queue_writer: MpscQueueWrap<CtxMessage<MSG, RESULT, STATE>>,
}

impl<KEY, STATE, MSG, RESULT> PendingInfo<KEY, STATE, MSG, RESULT> {
    fn int_add_message(&self, msg: CtxMessage<MSG, RESULT, STATE>) {
        self.queue_writer.offer(msg);
    }
}

/// Represents the thread safe client for the state machine.
pub struct StateMachineClient<MSG> {
    queue: MpscQueueWrap<MSG>,
}

async fn run<
    KEY: Hash + Eq + Send,
    STATE: Hash + Eq + Sync + Send + Clone,
    MODEL,
    MSG: Send,
    RESULT,
>(
    ctx: Arc<ContextContainer<KEY, STATE, MODEL, MSG, RESULT>>,
    states: Arc<HashMap<STATE, Box<dyn StatePersisted<KEY, STATE, MODEL, MSG, RESULT>>>>,
) {
    loop {
        if ctx.start() {
            ctx.acquired();
            loop {
                let ctx = ctx.clone();
                if let Some(m) = ctx.poll() {
                    match m {
                        CtxMessage::UserMessage { msg, future } => {
                            // Get the state
                            if let Some(state_ext) = states.get(&ctx.state()) {
                                let result = state_ext.handle_event(&msg, ctx.clone()).await;
                                match result {
                                    EventActionResult::DidAction { result } => {
                                        if let Some(f) = future {
                                            f.send(StateMachineResult::Ran(result));
                                        }
                                    }
                                    EventActionResult::Defer => {
                                        ctx.skip_message(msg, future);
                                    }
                                    EventActionResult::GoToState { state } => {
                                        if let Some(new_state) = states.get(&state) {
                                            state_ext.exit(&msg, ctx.clone()).await;
                                            ctx.set_state(state.clone());
                                            new_state.entry(&msg, ctx).await;
                                            if let Some(f) = future {
                                                f.send(StateMachineResult::ChangedState(
                                                    state.clone(),
                                                ));
                                            }
                                        } else {
                                            // TODO Unable to find the state.
                                        }
                                    }
                                    EventActionResult::Ignore => {
                                        if let Some(f) = future {
                                            f.send(StateMachineResult::Ignored);
                                        }
                                    }
                                }
                            } else {
                                // TODO figure out what to do here.
                            }
                        }
                    }
                } else {
                    break;
                }
            }
            ctx.completed();
            ctx.finish();
        }
        if ctx.peek().is_none() {
            break;
        } else {
            // Need to go around again.
        }
    }
}

impl<KEY: Hash + Eq, STATE: Hash + Eq + Clone + Send, MODEL, MSG: Send, RESULT>
    Context<KEY, STATE, MODEL, MSG, RESULT>
{
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

    /// Adds a message to the queue.
    pub fn add_message(&self, msg: MSG, future: MsgFuture<RESULT, STATE>) -> bool {
        self.queue_writer
            .offer(CtxMessage::UserMessage { future, msg })
    }

    fn int_add_message(&self, msg: CtxMessage<MSG, RESULT, STATE>) {
        self.queue_writer.offer(msg);
    }

    fn skip_message(&mut self, msg: MSG, future: MsgFuture<RESULT, STATE>) -> bool {
        self.queue_reader
            .skip(CtxMessage::UserMessage { msg, future });
        true
    }
}

unsafe impl<KEY: Hash + Eq, STATE, MODEL, MSG: Send, RESULT> Sync
    for Context<KEY, STATE, MODEL, MSG, RESULT>
{
}
unsafe impl<KEY: Hash + Eq, STATE, MODEL, MSG: Send, RESULT> Send
    for Context<KEY, STATE, MODEL, MSG, RESULT>
{
}

pub enum EventActionResult<STATE: Hash + Eq, RESULT> {
    GoToState { state: STATE },
    DidAction { result: RESULT },
    Ignore,
    Defer,
}

#[cfg(test)]
mod tests {

    use crate::{
        create_state_machine, Context, ContextContainer, EventActionResult, LoadInfo,
        StateMachineResult, StatePersisted,
    };
    use async_trait::async_trait;
    use futures::executor::block_on;
    use futures::Future;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::runtime::Handle;

    #[derive(Hash, PartialEq, Eq, Clone, Debug)]
    enum TestState {
        Start,
        Middle,
        End,
    }

    #[derive(PartialEq)]
    enum TestEvt {
        Middle,
        Message(String),
        GetCurrentState,
        End,
    }

    struct Model {
        my_id: u32,
    }

    enum ResultMy {
        Message,
        State(TestState),
    }

    #[tokio::test]
    pub async fn create_state_machine_test() {
        let state_machine = create_state_machine(
            |id| async move {
                LoadInfo {
                    current_state: TestState::Start,
                    id,
                    last_transition_id: 2,
                    model: Model { my_id: id },
                }
            },
            &mut vec![
                Box::new(StartState {}),
                Box::new(MiddleState {}),
                Box::new(EndState {}),
            ],
            1024,
            Handle::current(),
        );
        let result = state_machine.send(1, TestEvt::Middle).await.unwrap();
        match result {
            StateMachineResult::ChangedState(state) => {
                assert_eq!(TestState::Middle, state);
            }
            _ => {
                assert!(false);
            }
        }
        let state = state_machine
            .send(1, TestEvt::GetCurrentState)
            .await
            .unwrap();
        match state {
            StateMachineResult::Ran(r) => match &r {
                ResultMy::State(state) => {
                    assert_eq!(TestState::Middle, state.clone());
                }
                _ => {
                    panic!("invalid result type.");
                }
            },
            _ => {
                panic!("Did not run as expected");
            }
        }
    }

    struct StartState {}

    type MyState = StatePersisted<u32, TestState, Model, TestEvt, ResultMy>;
    type MyContext = Arc<ContextContainer<u32, TestState, Model, TestEvt, ResultMy>>;
    type MyEventActionResult = EventActionResult<TestState, ResultMy>;

    #[async_trait]
    impl StatePersisted<u32, TestState, Model, TestEvt, ResultMy> for StartState {
        fn state(&self) -> TestState {
            TestState::Start
        }

        async fn handle_event(&self, event: &TestEvt, ctx: MyContext) -> MyEventActionResult {
            match event {
                TestEvt::Middle => EventActionResult::GoToState {
                    state: TestState::Middle,
                },
                _ => EventActionResult::Ignore,
            }
        }

        async fn entry(&self, evt: &TestEvt, ctx: MyContext) {
            ctx.add_message(TestEvt::Message("Hi".to_owned()), None);
        }

        async fn exit(&self, evt: &TestEvt, ctx: MyContext) {}
    }

    struct MiddleState {}

    #[async_trait]
    impl StatePersisted<u32, TestState, Model, TestEvt, ResultMy> for MiddleState {
        fn state(&self) -> TestState {
            TestState::Middle
        }

        async fn handle_event(&self, event: &TestEvt, ctx: MyContext) -> MyEventActionResult {
            match event {
                TestEvt::GetCurrentState => EventActionResult::DidAction {
                    result: ResultMy::State(ctx.state().clone()),
                },
                _ => EventActionResult::Ignore,
            }
        }

        async fn entry(&self, evt: &TestEvt, ctx: MyContext) {}

        async fn exit(&self, evt: &TestEvt, ctx: MyContext) {}
    }

    struct EndState {}

    #[async_trait]
    impl StatePersisted<u32, TestState, Model, TestEvt, ResultMy> for EndState {
        fn state(&self) -> TestState {
            TestState::End
        }

        async fn handle_event(&self, event: &TestEvt, ctx: MyContext) -> MyEventActionResult {
            EventActionResult::Ignore
        }

        async fn entry(&self, evt: &TestEvt, ctx: MyContext) {}

        async fn exit(&self, evt: &TestEvt, ctx: MyContext) {}
    }
}
