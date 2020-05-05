//! A very simple actor for single writer principle.  Not meant to be the fastest but for simplicity.
use crate::queue::mpsc_queue::{MpscQueueReceive, MpscQueueWrap};
use async_trait::async_trait;
use futures::channel::oneshot;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::{fence, Ordering};
use std::sync::Arc;
use std::{cell::UnsafeCell, pin::Pin};

pub type ResultFuture<RESULT> = oneshot::Receiver<RESULT>;

/// A simple base message.
struct BaseMsg<MSG, RESULT: Send> {
    /// The message to send.
    pub message: MSG,
    /// The oneshot to use to send the value back.
    pub sender: oneshot::Sender<RESULT>,
}

#[async_trait]
pub trait ActorMessageHandler {
    /// The type for the message.
    type Message: Send;
    /// The starting context for the message.
    type Context;
    /// The result type for a processing a message.
    type Result: Send;

    /// Used to create a new actor.
    /// # Arguments
    /// `context` - The context for the actor to use to create.
    fn create(context: Self::Context) -> Self;

    /// Called to apply the change of the message.
    /// # Arguments
    /// `msg` - The message to the actor.
    async fn apply(&mut self, msg: Self::Message) -> Self::Result;
}

const IDLE_STATE: u32 = 0;
const RUNNING: u32 = 1;

struct ActorInt<MessageHandler: ActorMessageHandler> {
    // contains the message handler and the state.
    actor_message_handler: MessageHandlerGuard<MessageHandler>,
    /// The current active state of the actor.
    current_state: AtomicU32,
    /// The writer for the queue.
    writer: MpscQueueWrap<BaseMsg<MessageHandler::Message, MessageHandler::Result>>,
    /// The reader for the queue.
    reader: MpscQueueReceive<BaseMsg<MessageHandler::Message, MessageHandler::Result>>,
}

pub struct Actor<MessageHandler: ActorMessageHandler> {
    // Want this pinned in memory to prevent it from being moved since we are using it in different threads.  Also want it boxed so it's in the heap.
    actor: Arc<Pin<Box<UnsafeCell<ActorInt<MessageHandler>>>>>,
}

struct ActorGuard<MessageHandler: ActorMessageHandler> {
    // Want this pinned in memory to prevent it from being moved since we are using it in different threads.
    actor: Arc<Pin<Box<UnsafeCell<ActorInt<MessageHandler>>>>>,
}

impl<MessageHandler: ActorMessageHandler> ActorGuard<MessageHandler> {
    fn new(actor: Arc<Pin<Box<UnsafeCell<ActorInt<MessageHandler>>>>>) -> Self {
        Self { actor }
    }
}

unsafe impl<MessageHandler: ActorMessageHandler> Sync for ActorGuard<MessageHandler> {}
unsafe impl<MessageHandler: ActorMessageHandler> Send for ActorGuard<MessageHandler> {}

impl<MessageHandler: ActorMessageHandler + 'static> Actor<MessageHandler> {
    pub fn create(context: MessageHandler::Context, queue_size: usize) -> Self {
        Self {
            actor: Arc::new(Box::pin(UnsafeCell::new(ActorInt::create_actor(
                context, queue_size,
            )))),
        }
    }

    pub fn send_message(
        &self,
        message: MessageHandler::Message,
    ) -> ResultFuture<MessageHandler::Result> {
        let (sender, receiver) = oneshot::channel();
        // Add to the message queue.
        let msg = BaseMsg { message, sender };
        let actor = unsafe { &mut *self.actor.get() };
        if actor.send(msg) {
            // Need to spawn off the worker.
            let actor = ActorGuard::new(self.actor.clone());
            tokio::spawn(async move {
                // Need to make sure the actor drops before we await on the future.
                let future = {
                    let actor = unsafe { &mut *actor.actor.get() };
                    actor.run()
                };
                future.await;
            });
        }
        receiver
    }
}

struct MessageHandlerGuard<MessageHandler: ActorMessageHandler> {
    // Needs to be pinned in memory since we are using it from multiple threads.  Also want it boxed so it's in the heap.
    actor_message_handler: Pin<Box<UnsafeCell<MessageHandler>>>,
}

impl<MessageHandler: ActorMessageHandler> MessageHandlerGuard<MessageHandler> {
    
    async fn apply(&self, msg: BaseMsg<MessageHandler::Message, MessageHandler::Result>) {
        let future = {
            let handler = unsafe {&mut *self.actor_message_handler.get()};
            handler.apply(msg.message)
        };
        let r = future.await;
        msg.sender.send(r).unwrap_or_default();
    }
} 

unsafe impl<MessageHandler: ActorMessageHandler> Sync for MessageHandlerGuard<MessageHandler> {}
unsafe impl<MessageHandler: ActorMessageHandler> Send for MessageHandlerGuard<MessageHandler> {}

impl<MessageHandler: ActorMessageHandler> ActorInt<MessageHandler> {
    fn create_actor(context: MessageHandler::Context, queue_size: usize) -> Self {
        let (writer, reader) = MpscQueueWrap::new(queue_size);
        Self {
            actor_message_handler: MessageHandlerGuard{
                actor_message_handler: Box::pin(UnsafeCell::new(MessageHandler::create(context)))
            },
            current_state: AtomicU32::new(IDLE_STATE),
            writer,
            reader,
        }
    }

    /// Sends a message to the actor.
    /// # Arguments
    /// `msg` - The message to send to the actor.
    fn send(&self, msg: BaseMsg<MessageHandler::Message, MessageHandler::Result>) -> bool {
        self.writer.offer(msg);
        // Need a full memory barrier on success to make sure the queue poll value gets update across the threads.  Only care on the success.
        self.current_state
            .compare_exchange(IDLE_STATE, RUNNING, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
    }

    async fn run(&mut self) {
        loop {
            loop {
                if let Some(a) = self.reader.poll() {
                    self.actor_message_handler.apply(a).await;
                } else {
                    break;
                }
            }
            self.current_state.store(IDLE_STATE, Ordering::Relaxed);
            // Full barrier here to make sure everything is done before and handle stale values that maybe in the queue.
            fence(Ordering::SeqCst);
            if self.reader.peek().is_some()
                // Do the compare and swap to see if we win.  Only want ordering guarantees so it can't be relaxed on success.
                && self.current_state.compare_exchange(IDLE_STATE, RUNNING, Ordering::AcqRel, Ordering::Relaxed).is_ok()
            {
                // Go around again
            } else {
                break
            }
        }
    }
}

#[cfg(test)]
mod tests {
    
    use super::*;
    use async_trait::async_trait;

    #[derive(PartialEq, Debug)]
    enum MsgResult {
        Result1,
        Result2,
    }

    enum Msg {
        Msg1
    }

    struct Context {
        id: u32,
    }

    struct MyHandler {

        context: Context,
    }

    #[async_trait]
    impl ActorMessageHandler for MyHandler {
        
        type Message = Msg;
        /// The starting context for the message.
        type Context = Context;
        /// The result type for a processing a message.
        type Result =  MsgResult;

        /// Used to create a new actor.
        /// # Arguments
        /// `context` - The context for the actor to use to create.
        fn create(context: Self::Context) -> Self {
            Self {
                context,
            }
        }

        /// Called to apply the change of the message.
        /// # Arguments
        /// `msg` - The message to the actor.
        async fn apply(&mut self, msg: Self::Message) -> Self::Result {
            MsgResult::Result1
        }
    }

    #[tokio::test]
    pub async fn actor_test() {
        let actor = Actor::<MyHandler>::create(Context {
            id: 2
        }, 1024);
        let msg = Msg::Msg1;
        let result = actor.send_message(msg).await.unwrap();
        assert_eq!(result, MsgResult::Result1);
        let msg = Msg::Msg1;
        let result = actor.send_message(msg).await.unwrap();
        assert_eq!(result, MsgResult::Result1);
    }
}
