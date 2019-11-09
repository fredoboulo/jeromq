package zmq.socket.pubsub.tree;

import zmq.Msg;
import zmq.pipe.Pipe;

import java.nio.ByteBuffer;

public interface Tree
{
    interface TreeHandler
    {
        void added(byte[] data, int size, Pipe arg);
    }

    /**
     * Add key to the tree.
     *
     * @param msg the key message to add
     * @param start the 0-based starting index of the key inside the message.
     * @param size the size of the key.
     * @return true if this is a new item in the tree rather than a duplicate.
     */
    boolean add(Msg msg, int start, int size);

    /**
     * Remove key from the tree.
     *
     * @param msg the key message to remove
     * @param start the 0-based starting index of the key inside the message.
     * @param size the size of the key.
     * @return true if the item is actually removed from the tree.
     */
    boolean rm(Msg msg, int start, int size);

    /**
     * Check whether particular key is in the tree.
     *
     * @param key the key to check.
     * @return true if the key is in the tree, otherwise false.
     */
    boolean check(ByteBuffer key);

    /**
     * Apply the function supplied to each subscription in the tree.
     *
     * @param func the function to apply.
     * @param arg the pipe used as an argument for the applied function.
     */
    void apply(TreeHandler func, Pipe arg);
}
