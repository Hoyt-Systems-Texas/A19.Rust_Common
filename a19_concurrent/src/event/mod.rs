use std::sync::Arc;
use std::cell::UnsafeCell;
use std::sync::atomic::{ AtomicU32, AtomicPtr, Ordering };
use std::thread;
use std::collections::VecDeque;

const READER: u32 = 1;
const WRITER_PENDING: u32 = 2;
const WRITER: u32 = 3;

pub trait ApplyChanges<COL, EVENT> {

    fn apply(&self, map: &mut COL, event: &EVENT);
}
pub struct MrswEventProcessor<COL, EVENT, CHANGE: ApplyChanges<COL, EVENT>> {
    current_reader: AtomicPtr<Box<CollectionContainer<COL, EVENT>>>,
    col1: Box<CollectionContainer<COL, EVENT>>,
    col2: Box<CollectionContainer<COL, EVENT>>,
    apply_change: CHANGE
}

unsafe impl<COL, EVENT, CHANGE: ApplyChanges<COL, EVENT>> Sync for MrswEventProcessor<COL, EVENT, CHANGE> {}
unsafe impl<COL, EVENT, CHANGE: ApplyChanges<COL, EVENT>> Send for MrswEventProcessor<COL, EVENT, CHANGE> {}

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
    current_reader: AtomicPtr<Box<CollectionContainer<COL, EVENT>>>,
    current_writer: *mut Box<CollectionContainer<COL, EVENT>>,
    /// The first collection.
    col1: Box<CollectionContainer<COL, EVENT>>,
    /// The second collection.
    col2: Box<CollectionContainer<COL, EVENT>>,
    /// The action to run to apply the changes.
    apply_change: CHANGE
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
        let reader = self.current_reader.load(Ordering::Relaxed);        
        let col = unsafe {&mut (*self.current_writer).col};
        MrswCollection::apply_change(&self.apply_change, col, &event);
        unsafe {(*reader).event_stream.push_back(event)};
    }

    fn commit(&mut self) {
        let (reader, writer) = if self.col1.state.load(Ordering::Relaxed) == READER {
            (&mut self.col1, &mut self.col2)
        } else {
            (&mut self.col2, &mut self.col1)
        };

        writer.state.store(READER, Ordering::Relaxed);
        reader.state.store(WRITER_PENDING, Ordering::Relaxed);
        self.current_reader.store(writer, Ordering::Relaxed);
        self.current_writer = reader;
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

}
