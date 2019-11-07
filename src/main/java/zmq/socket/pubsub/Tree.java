package zmq.socket.pubsub;

import zmq.Msg;
import zmq.pipe.Pipe;
import zmq.util.Utils;

import java.nio.ByteBuffer;

public interface Tree
{
    interface ITrieHandler
    {
        void added(byte[] data, int size, Pipe arg);
    }

    //  Add key to the trie. Returns true if this is a new item in the trie
    //  rather than a duplicate.
    boolean add(Msg msg, int start, int size);

    //  Remove key from the trie. Returns true if the item is actually
    //  removed from the trie.
    boolean rm(Msg msg, int start, int size);

    //  Check whether particular key is in the trie.
    boolean check(ByteBuffer data);

    //  Apply the function supplied to each subscription in the trie.
    void apply(ITrieHandler func, Pipe arg);
}
