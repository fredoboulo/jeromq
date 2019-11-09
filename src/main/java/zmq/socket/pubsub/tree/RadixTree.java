package zmq.socket.pubsub.tree;

import zmq.Msg;
import zmq.pipe.Pipe;
import zmq.util.Draft;
import zmq.util.Utils;

import java.nio.ByteBuffer;

/**
 * This is a DRAFT method, and may change without notice.
 */
@Draft
public class RadixTree implements Tree
{
    private final RadixTreeBase tree = new RadixTreeBase();

    @Override
    public boolean add(Msg msg, int start, int size)
    {
        Utils.checkArgument(msg != null, "A null message cannot be added in a radix tree");
        ByteBuffer buf = msg.buf();
        buf.position(start);
        buf = buf.slice();
        return tree.add(buf);
    }

    @Override
    public boolean rm(Msg msg, int start, int size)
    {
        Utils.checkArgument(msg != null, "A null message cannot be removed from a radix tree");
        ByteBuffer buf = msg.buf();
        buf.position(start);
        buf = buf.slice();
        return tree.rm(buf);
    }

    @Override
    public boolean check(ByteBuffer data)
    {
        Utils.checkArgument(data != null, "A null data cannot be checked in a radix tree");
        return tree.check(data);
    }

    @Override
    public void apply(TreeHandler func, Pipe arg)
    {
        Utils.checkArgument(func != null, "A handler needs to be set");
        tree.apply((buf, pipe) -> func.added(buf, buf.length, pipe), Utils::toBytes, arg);
    }

    int size()
    {
        return tree.size();
    }
}
