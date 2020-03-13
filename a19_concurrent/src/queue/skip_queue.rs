use crate::queue::mpsc_queue::{MpscQueueReceive, MpscQueueWrap};
use std::collections::VecDeque;

enum ActiveQueue {
    Primary,
    SkipQueue,
}

pub struct SkipQueueReader<M> {
    queue_reader: MpscQueueReceive<M>,
    skip_queue: VecDeque<M>,
    current_queue: ActiveQueue,
}

unsafe impl<M> Send for SkipQueueReader<M> {}

pub fn create_skip_queue<M>(size: usize) -> (MpscQueueWrap<M>, SkipQueueReader<M>) {
    let (queue_writer, queue_reader) = MpscQueueWrap::<M>::new(size);
    let reader = SkipQueueReader {
        queue_reader,
        skip_queue: VecDeque::with_capacity(64),
        current_queue: ActiveQueue::Primary,
    };
    (queue_writer, reader)
}

impl<M> SkipQueueReader<M> {
    /// Used to get the value out of the queue.
    pub fn poll(&mut self) -> Option<M> {
        match self.current_queue {
            ActiveQueue::Primary => self.queue_reader.poll(),
            ActiveQueue::SkipQueue => {
                if self.skip_queue.len() > 0 {
                    self.skip_queue.pop_front()
                } else {
                    self.current_queue = ActiveQueue::Primary;
                    self.queue_reader.poll()
                }
            }
        }
    }

    /// Pushes back a message that should be skipped.
    /// # Arguments
    /// `m` - The message to push back.
    pub fn skip(&mut self, m: M) {
        self.skip_queue.push_back(m)
    }

    /// Switches to the skip queue if there are any values present.
    pub fn switch_to_skip(&mut self) {
        if self.skip_queue.len() > 0 {
            self.current_queue = ActiveQueue::SkipQueue
        }
    }
}

#[cfg(test)]
mod test {

    use crate::queue::skip_queue::crate_skip_queue;

    #[test]
    pub fn skip_queue_test() {
        let (writer, mut reader) = create_skip_queue::<i32>(128);
        writer.offer(1);
        writer.offer(2);
        reader.skip(-1);
        assert_eq!(1, reader.poll().unwrap());
        reader.switch_to_skip();
        assert_eq!(-1, reader.poll().unwrap());
        assert_eq!(2, reader.poll().unwrap());
    }
}
