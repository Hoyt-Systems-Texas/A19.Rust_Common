use std::sync::atomic::{ AtomicU32, AtomicPtr };
use std::thread;
use std::collections::VecDeque;

const READER: u32 = 1;
const WRITER: u32 = 2;

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

unsafe impl<COL, EVENT> Sync for CollectionContainer<COL, EVENT> {}
unsafe impl<COL, EVENT> Send for CollectionContainer<COL, EVENT> {}
