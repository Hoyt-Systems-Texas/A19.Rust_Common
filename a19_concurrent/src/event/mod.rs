use std::sync::Arc;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::cell::{ UnsafeCell };
use std::sync::atomic::{ AtomicU32, AtomicPtr, Ordering };
use std::thread;
use std::collections::VecDeque;
use std::ops::DerefMut;

const READER: u32 = 1;
const WRITER_PENDING: u32 = 2;
const WRITER: u32 = 3;

pub trait ApplyChanges<COL, EVENT> {

    fn apply(&self, map: &mut COL, event: &EVENT);
}

struct CollectionContainer<COL, EVENT> {
    col: COL,
    reader_count: AtomicU32,
    state: AtomicU32,
    event_stream: VecDeque<EVENT>
}

impl<COL, EVENT> CollectionContainer<COL, EVENT> {
    
    fn new(
        col: COL,
        state: u32,
        event_stream_size: usize) -> Self {
        CollectionContainer {
            col,
            reader_count: AtomicU32::new(0),
            state: AtomicU32::new(state),
            event_stream: VecDeque::with_capacity(event_stream_size)
        }
    }
}

unsafe impl<COL, EVENT> Sync for CollectionContainer<COL, EVENT> {}
unsafe impl<COL, EVENT> Send for CollectionContainer<COL, EVENT> {}

/// Represents a generic collection you can apply an event stream to.
struct MrswCollection<COL, EVENT, CHANGE: ApplyChanges<COL, EVENT>> {
    /// The current reader.
    current_reader: AtomicPtr<CollectionContainer<COL, EVENT>>,
    /// The action to run to apply the changes.
    current_writer: AtomicPtr<CollectionContainer<COL, EVENT>>,
    col1: CollectionContainer<COL, EVENT>,
    col2: CollectionContainer<COL, EVENT>,
    apply_change: CHANGE
}

pub fn create_mrsw_collection<COL, EVENT, CHANGE: ApplyChanges<COL, EVENT>>(
    col1: COL,
    col2: COL,
    apply_change: CHANGE
) -> (MrswCollectionReader<COL, EVENT, CHANGE>, MrswCollectionWriter<COL, EVENT, CHANGE>) {
    let mut con1 = CollectionContainer::new(
        col1,
        READER,
        1024);
    let mut con2 = CollectionContainer::new(
        col2,
        WRITER,
        1024);
    let mut col = MrswCollection {
                current_reader: AtomicPtr::new(&mut con1),
                current_writer: AtomicPtr::new(&mut con2),
                col1: con1,
                col2: con2,
                apply_change
    };
    let col_arc = Arc::new(
        UnsafeCell::new(
            col));
    let v = unsafe {&mut *col_arc.get()};
    v.current_reader.store(&mut v.col1, Ordering::Relaxed);
    v.current_writer.store(&mut v.col2, Ordering::Relaxed);
    (MrswCollectionReader {
        map: col_arc.clone()
    }, MrswCollectionWriter {
        map: col_arc
    })
}


pub struct MrswCollectionReader<COL, EVENT, CHANGE: ApplyChanges<COL, EVENT>> {
    map: Arc<UnsafeCell<MrswCollection<COL, EVENT, CHANGE>>>
}

impl<COL, EVENT, CHANGE: ApplyChanges<COL, EVENT>> MrswCollectionReader<COL, EVENT, CHANGE> {

    pub fn get<f, r>(&self, act: f) -> r 
        where f:FnOnce(&COL) -> r {
        let v = unsafe {&mut *self.map.get()};
        v.get(act)
    }
}

unsafe impl<COL, EVENT, CHANGE: ApplyChanges<COL, EVENT>> Sync for MrswCollectionReader<COL, EVENT, CHANGE> {}
unsafe impl<COL, EVENT, CHANGE: ApplyChanges<COL, EVENT>> Send for MrswCollectionReader<COL, EVENT, CHANGE> {}

pub struct MrswCollectionWriter<COL, EVENT, CHANGE: ApplyChanges<COL, EVENT>> {
    map: Arc<UnsafeCell<MrswCollection<COL, EVENT, CHANGE>>>
}

impl<COL, EVENT, CHANGE: ApplyChanges<COL, EVENT>> MrswCollectionWriter<COL, EVENT, CHANGE> {

    pub fn add_event(&mut self, event: EVENT) {
        let v = unsafe {&mut *self.map.get()};
        v.add_event(event);
    }

    pub fn commit(&mut self) {
        let v = unsafe {&mut *self.map.get()};
        v.commit();
    }
}

unsafe impl<COL, EVENT, CHANGE: ApplyChanges<COL, EVENT>> Send for MrswCollectionWriter<COL, EVENT, CHANGE> {}

impl<COL, EVENT, CHANGE: ApplyChanges<COL, EVENT>> MrswCollection<COL, EVENT, CHANGE> {

    fn add_event(&mut self, event: EVENT) {
        unsafe {
            let writer = &mut *self.current_writer.load(Ordering::Relaxed);
            MrswCollection::apply_change(&self.apply_change, &mut writer.col, &event);
        }
        unsafe {
            let reader = &mut *self.current_reader.load(Ordering::Relaxed);
            reader.event_stream.push_back(event);
        }
    }

    fn commit(&mut self) {
        let reader = unsafe {&mut *self.current_reader.load(Ordering::Relaxed)};
        let writer = unsafe {&mut *self.current_writer.load(Ordering::Relaxed)};
        writer.state.store(READER, Ordering::Relaxed);
        reader.state.store(WRITER_PENDING, Ordering::Relaxed);
        self.current_reader.store(writer, Ordering::Relaxed);
        self.current_writer.store(reader, Ordering::Relaxed);
        loop {
            if reader.reader_count.load(Ordering::Relaxed) == 0 {
                reader.state.store(WRITER, Ordering::Relaxed);
                break
            } else {
                thread::yield_now()
            }
        }

        loop {
            let event = reader.event_stream.pop_front();
            match event {
                Some(e) => {
                    MrswCollection::apply_change(&self.apply_change, &mut reader.col, &e);
                },
                None => {
                    break;
                }
            }
        }
    }

    fn apply_change(
        apply_change: &CHANGE,
        col: &mut COL,
        event: &EVENT) {
        apply_change.apply(col, event);
    }

    fn get<F, R>(&mut self, act: F) -> R 
        where F:FnOnce(&COL) -> R
    {
        loop {
            let reader = self.current_reader.load(Ordering::Relaxed);
            unsafe{(*reader).reader_count.fetch_add(1, Ordering::Relaxed)};
            if unsafe {(*reader).state.load(Ordering::SeqCst)} == READER {
                let v = unsafe{&(*reader).col};
                let r = act(v);
                unsafe{(*reader).reader_count.fetch_sub(1, Ordering::Relaxed)};
                break r
            } else {
                unsafe{(*reader).reader_count.fetch_sub(1, Ordering::Relaxed)};
                thread::yield_now()
            }
        }
    }
}

#[cfg(test)]
mod test {

    use crate::event::{ MrswCollectionWriter, MrswCollectionReader, create_mrsw_collection, ApplyChanges };

    struct ValueTest {
        id: u32
    }

    struct ApplyTest {

    }

    enum EventTest {
        MyEvent(u32)
    }

    #[test]
    pub fn create_mrsw_collection_test() {
        let apply_change = ApplyTest{};
        let (reader, mut writer) = create_mrsw_collection(
            ValueTest{
                id: 1
            },
            ValueTest{
                id:1
            },
            apply_change
        );
        writer.add_event(EventTest::MyEvent(2));        
        writer.commit();
        let r = reader.get(|v|{
            v.id
        });
        assert_eq!(2, r);
    }

    impl ApplyChanges<ValueTest, EventTest> for ApplyTest {

        fn apply(&self, map: &mut ValueTest, event: &EventTest) {
            match event {
                EventTest::MyEvent(val) => {
                    map.id = val.clone();
                }
            }
        }
    }

}
