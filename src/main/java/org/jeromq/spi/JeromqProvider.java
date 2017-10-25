package org.jeromq.spi;

import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.Selector;

import zmq.Msg;
import zmq.SocketBase;
import zmq.ZMQ;
import zmq.api.AContext;
import zmq.api.AEvent;
import zmq.api.AMechanism;
import zmq.api.AMetadata;
import zmq.api.AMsg;
import zmq.api.APollItem;
import zmq.api.AProvider;
import zmq.api.ASocket;
import zmq.io.Metadata;
import zmq.io.mechanism.Mechanisms;
import zmq.io.mechanism.curve.Curve;
import zmq.poll.PollItem;
import zmq.util.Z85;

public class JeromqProvider implements AProvider
{
    public JeromqProvider()
    {
        super();
    }

    @Override
    public AContext context(int ioThreads)
    {
        return ZMQ.init(ioThreads);
    }

    @Override
    public AMsg msg(byte[] data)
    {
        return new Msg(data);
    }

    @Override
    public AMsg msg(ByteBuffer data)
    {
        return new Msg(data);
    }

    @Override
    public String[] keypairZ85()
    {
        return new Curve().keypairZ85();
    }

    @Override
    public String z85Encode(byte[] key)
    {
        return Curve.z85EncodePublic(key);
    }

    @Override
    public byte[] z85Decode(String key)
    {
        return Z85.decode(key);
    }

    @Override
    public AMechanism findMechanism(Object mechanism)
    {
        assert (mechanism instanceof Mechanisms);

        switch ((Mechanisms) mechanism) {
        case NULL:
            return AMechanism.NULL;
        case PLAIN:
            return AMechanism.PLAIN;
        case CURVE:
            return AMechanism.CURVE;
        case GSSAPI:
            return AMechanism.GSSAPI;
        default:
            return null;
        }
    }

    @Override
    public APollItem pollItem(ASocket socket, int ops)
    {
        assert (socket instanceof SocketBase);
        return new PollItem((SocketBase) socket, ops);
    }

    @Override
    public APollItem pollItem(SelectableChannel channel, int ops)
    {
        return new PollItem(channel, ops);
    }

    @Override
    public int poll(Selector selector, APollItem[] items, int size, long timeout)
    {
        PollItem[] its = new PollItem[size];
        for (int idx = 0; idx < size; ++idx) {
            assert (items[idx] instanceof PollItem);
            its[idx] = (PollItem) items[idx];
        }
        return ZMQ.poll(selector, its, size, timeout);
    }

    @Override
    public boolean proxy(ASocket frontend, ASocket backend, ASocket capture, ASocket control)
    {
        assert (frontend instanceof SocketBase);
        assert (backend instanceof SocketBase);
        assert (capture instanceof SocketBase || capture == null);
        assert (control instanceof SocketBase || control == null);
        return ZMQ.proxy((SocketBase) frontend, (SocketBase) backend, (SocketBase) capture, (SocketBase) control);
    }

    @Override
    public AMetadata metadata()
    {
        return new Metadata();
    }

    @Override
    public AEvent read(ASocket socket, int flags)
    {
        assert (socket instanceof SocketBase);
        return zmq.ZMQ.Event.read((SocketBase) socket, flags);
    }

    @Override
    public int versionMajor()
    {
        return zmq.ZMQ.ZMQ_VERSION_MAJOR;
    }

    @Override
    public int versionMinor()
    {
        return zmq.ZMQ.ZMQ_VERSION_MINOR;
    }

    @Override
    public int versionPatch()
    {
        return zmq.ZMQ.ZMQ_VERSION_PATCH;
    }
}
