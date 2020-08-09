# Input/Output buffer

We need to have an input and out buffer.  When we accept a message onto the buffer, need to have a way to signal we are done writting atomically.  If we decide to use flatbuffers, would need to have it edittable so we can flip a flag to signal we are done writting th message onto the output stream.

## Options For Flipping Done

We can write the size of the message at the end of the body.  This is how some messaging systems work and is serial so it would be very fast.

Flip a bit on the top of the header.  Would likely still be fast but could trigger a load from another cache.

~~~
Flattbuffers Header (Has the body size.  It doesn't include any of the alignment.)
Body
Alignment - 4
total-size (Body Size.  Needs to  have a special value if the body size is 0.  Could just put the max size since 0 will be used as pending.)
~~~

## Body Alignment

We need to have the body aligned in the message and the alignment would need to include the message size.  The current safe cache line size would be 64 bytes.  The total size of the message - 4 to figure out the alignment.  Can make the alignment size smaller due to the header being present.

## Header

We want the header to be a fixed size.  Could just make it one cache line or total cache line.

## Handle Writes on the Client

Create a type with a byte buffer.  Would get more complicated with things like strings.

## Reusing the Buffer

On the client we need to know when we are done with the message and zero it out.  Also, need to have rules on what to do if we never received the whole message and timeout so we don't end up blocking for a long time.

# Server Side

We can have different messages received by the server.  Some will be need to be replicated. 

## Server side buffer

We have slightly different requirements for the message.  Likely would have multiple buffers.

* Send a message to a client
* Receiving a message from a client
* Server to Server communication.  Liekly would be handled differently since this would likey be used for state replication and caching.