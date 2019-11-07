package zmq.socket.pubsub.radix;

import zmq.Msg;
import zmq.pipe.Pipe;
import zmq.socket.pubsub.Tree;

import java.nio.ByteBuffer;

public class MsgRadixTree implements Tree
{
    private final RadixTree tree = new RadixTree();

    @Override
    public boolean add(Msg msg, int start, int size)
    {
        return tree.add(msg.buf());
    }

    @Override
    public boolean rm(Msg msg, int start, int size)
    {
        return tree.rm(msg.buf());
    }

    @Override
    public boolean check(ByteBuffer data)
    {
        return tree.check(data);
    }

    @Override
    public void apply(ITrieHandler func, Pipe arg)
    {
        tree.apply((buf, pipe) -> func.added(buf, buf.length, pipe), tree::toBytes, arg);
    }
}
