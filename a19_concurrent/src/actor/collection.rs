use crate::queue::mpsc_queue::{MpscQueueWrap, MpscQueueReceive};
use crate::event::*;
use super::{ Actor, ActorMessageHandler };
use std::sync::Arc;
use std::{hash::Hash, collections::HashMap};
use async_trait::async_trait;
use futures::channel::oneshot;
use futures::FutureExt;
use futures::future;
use std::{pin::Pin, cell::RefCell};

enum ActorEvent<K: Eq + Hash + Clone,  ACTOR: ActorMessageHandler> {
    AddActor(K, Arc<Actor<ACTOR>>, Arc<RefCell<Option<oneshot::Sender<Option<Arc<Actor<ACTOR>>>>>>>)
}

#[derive(Clone)]
struct ActorContext {
    
}

/// Represents a collection of actor.
#[derive(Clone)]
struct ActorCollectionInt<K: Eq + Hash + Clone, V: ActorMessageHandler> {
    actor_map: HashMap<K, Arc<Actor<V>>>,
}

impl<K: Eq + Hash + Clone, V: ActorMessageHandler> ConcurrentCollection for ActorCollectionInt<K, V> {
    type Event = ActorEvent<K, V>;
    type Context = ActorContext;

    fn create(context: Self::Context) -> Self {
        Self {
            actor_map: HashMap::with_capacity(1024),
        }
    }

    fn apply(&mut self, event: &Self::Event) {
        match event {
            ActorEvent::AddActor(key, actor, sender) => {
                if let Some(actor) = self.actor_map.get(&key) {
                    if let Some(sender) = sender.borrow_mut().take() {
                        sender.send(Some(actor.clone()));
                    }
                } else {
                    self.actor_map.insert(key.clone(), actor.clone());
                    if let Some(sender) = sender.borrow_mut().take() {
                        sender.send(Some(actor.clone()));
                    }
                }
            }
        }
    }
}

struct MessageWriter<K: Eq + Hash + Clone, V: ActorMessageHandler> {
    writer: MrswCollectionWriter<ActorCollectionInt<K, V>>,
}

struct MessageWriterContext<K: Eq + Hash + Clone, V: ActorMessageHandler> {
    writer: MrswCollectionWriter<ActorCollectionInt<K, V>>,
}

enum MessageWriterMsg<K: Eq + Hash + Clone, V: ActorMessageHandler> {
    AddActor(K, V::Context, oneshot::Sender<Option<Arc<Actor<V>>>>),
}

type MessageWriterActor<K: Eq + Hash + Clone, V: ActorMessageHandler> = Actor<MessageWriter<K, V>>;

#[async_trait]
impl<K: Eq + Hash + Clone + Send, V: ActorMessageHandler + 'static> ActorMessageHandler for MessageWriter<K, V>
    where <V as ActorMessageHandler>::Context: std::marker::Send
{
    type Message = MessageWriterMsg<K, V>;
    type Context = MessageWriterContext<K, V>;
    type Result = ();

    fn create(context: Self::Context) -> Self {
        Self {
            writer: context.writer,
        }
    }

    async fn apply(&mut self, msg: Self::Message) -> Self::Result {
        match msg {
            MessageWriterMsg::AddActor(key, context, sender) => {
                let actor = Arc::new(Actor::<V>::create(context, 128));
                self.writer.add_event(ActorEvent::AddActor(
                    key, actor, Arc::new(RefCell::new(Some(sender)))));
                self.writer.commit();
            }
        }
        ()
    }

}

pub struct ActorCollection<K: Eq + Hash + Clone + Send, V: ActorMessageHandler + 'static>
    where <V as ActorMessageHandler>::Context: std::marker::Send
{
    reader: MrswCollectionReader<ActorCollectionInt<K, V>>,
    writer_actor: Arc<MessageWriterActor<K, V>>,
}


#[async_trait]
pub trait PersistedActor<K: Eq + Hash + Clone, V: ActorMessageHandler> {

    async fn load(actor_id: K) -> Option<V::Context>;
}

impl<K: Eq + Hash + Clone + Send + Sync + 'static, V: ActorMessageHandler + PersistedActor<K, V> + 'static> ActorCollection<K, V>
    where <V as ActorMessageHandler>::Context: std::marker::Send
{

    pub fn new() -> Self {
        let (reader, writer) = create_mrsw_collection(ActorContext {});
        Self {
            reader,
            writer_actor: Arc::new(Actor::create(MessageWriterContext{writer}, 1024)),
        }
    }

    pub fn actor(&self, key: K) -> oneshot::Receiver<Option<Arc<Actor<V>>>> {
        let (sender, receiver) = oneshot::channel();
        let temp_key = key.clone();
        if let Some(actor) = self.reader.get(move |map| {
            let key = key;
            let actor = map.actor_map.get(&key);
            if let Some(actor) = actor {
                Some(actor.clone())
            } else {
                None
            }
        }) {
            sender.send(Some(actor));
        } else {
            // TODO load the writer.
            let writer_actor = self.writer_actor.clone();
            tokio::spawn(async move {
                if let Some(actor_value) = V::load(temp_key.clone()).await {
                    let message = MessageWriterMsg::AddActor(temp_key, actor_value, sender);
                    writer_actor.send_message(message).await;
                } else {
                    sender.send(None);
                }
            });
        }
        receiver
    }
}

