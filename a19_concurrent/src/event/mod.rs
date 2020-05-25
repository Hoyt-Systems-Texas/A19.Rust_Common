use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering};
use std::sync::Arc;
use std::thread;

const READER: u32 = 1;
const WRITER_PENDING: u32 = 2;
const WRITER: u32 = 3;

pub trait ConcurrentCollection {
    type Event;
    type Context: Clone;

    /// Used to create the collection we are updating.
    fn create(context: Self::Context) -> Self;

    /// Applies an event on the data structure.
    fn apply(&mut self, event: &Self::Event);
}

struct CollectionContainer<C: ConcurrentCollection> {
    col: C,
    reader_count: AtomicU32,
    state: AtomicU32,
    event_stream: VecDeque<C::Event>,
}

impl<C: ConcurrentCollection> CollectionContainer<C> {
    fn new(context: C::Context, state: u32, event_stream_size: usize) -> Self {
        CollectionContainer {
            col: C::create(context),
            reader_count: AtomicU32::new(0),
            state: AtomicU32::new(state),
            event_stream: VecDeque::with_capacity(event_stream_size),
        }
    }
}

unsafe impl<C: ConcurrentCollection> Sync for CollectionContainer<C> {}
unsafe impl<C: ConcurrentCollection> Send for CollectionContainer<C> {}

/// Represents a generic collection you can apply an event stream to.
struct MrswCollection<C: ConcurrentCollection> {
    /// The current reader.
    current_reader: AtomicPtr<CollectionContainer<C>>,
    /// The action to run to apply the changes.
    current_writer: AtomicPtr<CollectionContainer<C>>,
    col1: CollectionContainer<C>,
    col2: CollectionContainer<C>,
}

pub fn create_mrsw_collection<C: ConcurrentCollection>(
    context: C::Context,
) -> (MrswCollectionReader<C>, MrswCollectionWriter<C>) {
    let mut con1 = CollectionContainer::new(context.clone(), READER, 1024);
    let mut con2 = CollectionContainer::new(context, WRITER, 1024);
    let col = MrswCollection {
        current_reader: AtomicPtr::new(&mut con1),
        current_writer: AtomicPtr::new(&mut con2),
        col1: con1,
        col2: con2,
    };
    let col_arc = Arc::new(UnsafeCell::new(col));
    let v = unsafe { &mut *col_arc.get() };
    v.current_reader.store(&mut v.col1, Ordering::Release);
    v.current_writer.store(&mut v.col2, Ordering::Release);
    (
        MrswCollectionReader {
            map: col_arc.clone(),
        },
        MrswCollectionWriter { map: col_arc },
    )
}

pub struct MrswCollectionReader<C: ConcurrentCollection> {
    map: Arc<UnsafeCell<MrswCollection<C>>>,
}

impl<C: ConcurrentCollection> MrswCollectionReader<C> {
    pub fn get<F, R>(&self, act: F) -> R
    where
        F: FnOnce(&C) -> R,
    {
        let v = unsafe { &mut *self.map.get() };
        v.get(act)
    }
}

unsafe impl<C: ConcurrentCollection> Sync for MrswCollectionReader<C> {}
unsafe impl<C: ConcurrentCollection> Send for MrswCollectionReader<C> {}

pub struct MrswCollectionWriter<C: ConcurrentCollection> {
    map: Arc<UnsafeCell<MrswCollection<C>>>,
}

impl<C: ConcurrentCollection> MrswCollectionWriter<C> {
    pub fn add_event(&mut self, event: C::Event) {
        let v = unsafe { &mut *self.map.get() };
        v.add_event(event);
    }

    pub fn commit(&mut self) {
        let v = unsafe { &mut *self.map.get() };
        v.commit();
    }
}

unsafe impl<C: ConcurrentCollection> Send for MrswCollectionWriter<C> {}

impl<C: ConcurrentCollection> MrswCollection<C> {
    fn add_event(&mut self, event: C::Event) {
        unsafe {
            let writer = &mut *self.current_writer.load(Ordering::Acquire);
            writer.col.apply(&event);
        }
        unsafe {
            let reader = &mut *self.current_reader.load(Ordering::Acquire);
            reader.event_stream.push_back(event);
        }
    }

    fn commit(&mut self) {
        let reader = unsafe { &mut *self.current_reader.load(Ordering::Acquire) };
        let writer = unsafe { &mut *self.current_writer.load(Ordering::Acquire) };
        writer.state.store(READER, Ordering::Release);
        reader.state.store(WRITER_PENDING, Ordering::Release);
        self.current_reader.store(writer, Ordering::Release);
        self.current_writer.store(reader, Ordering::Release);
        loop {
            if reader.reader_count.load(Ordering::Acquire) == 0 {
                reader.state.store(WRITER, Ordering::Release);
                break;
            } else {
                thread::yield_now()
            }
        }

        loop {
            let event = reader.event_stream.pop_front();
            match event {
                Some(e) => {
                    reader.col.apply(&e);
                }
                None => {
                    break;
                }
            }
        }
    }

    fn get<F, R>(&mut self, act: F) -> R
    where
        F: FnOnce(&C) -> R,
    {
        loop {
            let reader = self.current_reader.load(Ordering::Acquire);
            unsafe { (*reader).reader_count.fetch_add(1, Ordering::Relaxed) };
            if unsafe { (*reader).state.load(Ordering::Acquire) } == READER {
                let v = unsafe { &(*reader).col };
                let r = act(v);
                unsafe { (*reader).reader_count.fetch_sub(1, Ordering::Release) };
                break r;
            } else {
                unsafe { (*reader).reader_count.fetch_sub(1, Ordering::Release) };
                thread::yield_now()
            }
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[derive(Clone)]
    struct ValueTest {
        id: u32,
    }

    struct ApplyTest {
        value: ValueTest,
    }

    enum EventTest {
        MyEvent(u32),
    }

    #[test]
    pub fn create_mrsw_collection_test() {
        let value = ValueTest { id: 1 };
        let (reader, mut writer) = create_mrsw_collection(value);
        writer.add_event(EventTest::MyEvent(2));
        writer.commit();
        let r = reader.get(|v: &ApplyTest| v.value.id);
        assert_eq!(2, r);
    }

    impl ConcurrentCollection for ApplyTest {
        type Event = EventTest;
        type Context = ValueTest;

        fn create(context: ValueTest) -> Self {
            Self { value: context }
        }

        fn apply(&mut self, event: &EventTest) {
            match event {
                EventTest::MyEvent(val) => {
                    self.value.id = val.clone();
                }
            }
        }
    }
}
