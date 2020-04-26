
# Table of Contents

1.  [Raft Protocol](#orgfd48fea)
    1.  [Message Stream](#org3477411)


<a id="orgfd48fea"></a>

# Raft Protocol

The goal of this raft protocol implementation is to have a very fast implementation and is easy to understand.  Messages are sent in batches for faster processing and easy to create snapshots if using in-memory data structure.  Current goal is to have something that works with communication for each node.  


<a id="org3477411"></a>

## Message Stream

Messages are stored in a memory map file and is a fixed size.  Needs to be designed where you can have multiple writers to the stream which can be doen with `AtomicUsize`.

