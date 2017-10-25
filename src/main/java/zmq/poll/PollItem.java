package zmq.poll;

import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;

import zmq.SocketBase;
import zmq.ZMQ;
import zmq.api.APollItem;

public class PollItem implements APollItem
{
    private final SocketBase        socket;
    private final SelectableChannel channel;
    private final int               zinterest;
    private final int               interest;
    private int                     ready;

    public PollItem(SocketBase socket, int ops)
    {
        this(socket, null, ops);
    }

    public PollItem(SelectableChannel channel, int ops)
    {
        this(null, channel, ops);
    }

    private PollItem(SocketBase socket, SelectableChannel channel, int ops)
    {
        this.socket = socket;
        this.channel = channel;
        this.zinterest = ops;
        this.interest = init(ops);
    }

    private int init(int ops)
    {
        int interest = 0;
        if ((ops & ZMQ.ZMQ_POLLIN) > 0) {
            interest |= SelectionKey.OP_READ;
        }
        if ((ops & ZMQ.ZMQ_POLLOUT) > 0) {
            if (socket != null) { // ZMQ Socket get readiness from the mailbox
                interest |= SelectionKey.OP_READ;
            }
            else {
                interest |= SelectionKey.OP_WRITE;
            }
        }
        this.ready = 0;
        return interest;
    }

    @Override
    public final boolean isReadable()
    {
        return (ready & ZMQ.ZMQ_POLLIN) > 0;
    }

    @Override
    public final boolean isWritable()
    {
        return (ready & ZMQ.ZMQ_POLLOUT) > 0;
    }

    @Override
    public final boolean isError()
    {
        return (ready & ZMQ.ZMQ_POLLERR) > 0;
    }

    @Override
    public final SocketBase getSocket()
    {
        return socket;
    }

    @Override
    public final SelectableChannel getRawSocket()
    {
        return channel;
    }

    public final SelectableChannel getChannel()
    {
        if (socket != null) {
            //  If the poll item is a 0MQ socket we are interested in input on the
            //  notification file descriptor retrieved by the ZMQ_FD socket option.
            return socket.getFD();
        }
        else {
            //  Else, the poll item is a raw file descriptor. Convert the poll item
            //  events to the appropriate fd_sets.
            return channel;
        }
    }

    public final int interestOps()
    {
        return interest;
    }

    @Deprecated
    public final int zinterestOps()
    {
        return zinterest;
    }

    @Override
    public final boolean hasEvent(int events)
    {
        return (zinterest & events) > 0;
    }

    public final int interestOps(int ops)
    {
        init(ops);
        return interest;
    }

    public final int readyOps(SelectionKey key, int nevents)
    {
        ready = 0;

        if (socket != null) {
            //  The poll item is a 0MQ socket. Retrieve pending events
            //  using the ZMQ_EVENTS socket option.
            long events = socket.getSocketOpt(ZMQ.ZMQ_EVENTS);
            if (events < 0) {
                return -1;
            }

            if ((zinterest & ZMQ.ZMQ_POLLOUT) > 0 && (events & ZMQ.ZMQ_POLLOUT) > 0) {
                ready |= ZMQ.ZMQ_POLLOUT;
            }
            if ((zinterest & ZMQ.ZMQ_POLLIN) > 0 && (events & ZMQ.ZMQ_POLLIN) > 0) {
                ready |= ZMQ.ZMQ_POLLIN;
            }
        }
        //  Else, the poll item is a raw file descriptor, simply convert
        //  the events to zmq_pollitem_t-style format.
        else if (nevents > 0) {
            if (key.isReadable()) {
                ready |= ZMQ.ZMQ_POLLIN;
            }
            if (key.isWritable()) {
                ready |= ZMQ.ZMQ_POLLOUT;
            }
            if (!key.isValid() || key.isAcceptable() || key.isConnectable()) {
                ready |= ZMQ.ZMQ_POLLERR;
            }
        }

        return ready;
    }

    @Override
    public final int readyOps()
    {
        return ready;
    }
}
