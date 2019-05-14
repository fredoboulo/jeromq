package zmq;

import zmq.io.IEngine;
import zmq.io.IOThread;
import zmq.io.SessionBase;
import zmq.pipe.Pipe;
import zmq.pipe.YPipeBase;

//  Base class for all objects that participate in inter-thread
//  communication.
public abstract class ZObject
{
    //  Context provides access to the global state.
    protected final Ctx ctx;

    //  Thread ID of the thread the object belongs to.
    private int tid;

    protected ZObject(Ctx ctx, int tid)
    {
        this.ctx = ctx;
        this.tid = tid;
    }

    protected ZObject(ZObject parent)
    {
        this(parent.ctx, parent.tid);
    }

    public final int getTid()
    {
        return tid;
    }

    protected final void setTid(int tid)
    {
        this.tid = tid;
    }

    protected final Ctx getCtx()
    {
        return ctx;
    }

    @SuppressWarnings("unchecked")
    final void processCommand(Command cmd)
    {
        //        System.out.println(Thread.currentThread().getName() + ": Processing command " + cmd);
        switch (cmd.type) {
        case ACTIVATE_READ:
            processActivateRead();
            break;

        case ACTIVATE_WRITE:
            processActivateWrite((Long) cmd.arg);
            break;

        case STOP:
            processStop((Integer) cmd.arg);
            break;

        case PLUG:
            processPlug();
            processSeqnum();
            break;

        case OWN:
            processOwn((Own) cmd.arg);
            processSeqnum();
            break;

        case ATTACH:
            processAttach((IEngine) cmd.arg);
            processSeqnum();
            break;

        case BIND:
            processBind((Pipe) cmd.arg);
            processSeqnum();
            break;

        case HICCUP:
            processHiccup((YPipeBase<Msg>) cmd.arg);
            break;

        case PIPE_TERM:
            processPipeTerm();
            break;

        case PIPE_TERM_ACK:
            processPipeTermAck((Pipe.State) cmd.arg);
            break;

        case TERM_REQ:
            processTermReq((Own) cmd.arg);
            break;

        case TERM:
            processTerm((Integer) cmd.arg);
            break;

        case TERM_ACK:
            processTermAck((ZObject) cmd.arg);
            break;

        case REAP:
            processReap((ZObject) cmd.arg);
            break;

        case REAPED:
            processReaped((ZObject) cmd.arg);
            break;

        case INPROC_CONNECTED:
            processSeqnum();
            break;

        case DONE:
        default:
            throw new IllegalArgumentException();
        }
    }

    final boolean registerEndpoint(String addr, Ctx.Endpoint endpoint)
    {
        return ctx.registerEndpoint(addr, endpoint);
    }

    final boolean unregisterEndpoint(String addr, SocketBase socket)
    {
        return ctx.unregisterEndpoint(addr, socket);
    }

    final void unregisterEndpoints(SocketBase socket)
    {
        ctx.unregisterEndpoints(socket);
    }

    protected final Ctx.Endpoint findEndpoint(String addr)
    {
        return ctx.findEndpoint(addr);
    }

    final void pendConnection(String addr, Ctx.Endpoint endpoint, Pipe[] pipes)
    {
        ctx.pendConnection(addr, endpoint, pipes);
    }

    final void connectPending(String addr, SocketBase bindSocket)
    {
        ctx.connectPending(addr, bindSocket);
    }

    protected final void destroySocket(SocketBase socket)
    {
        ctx.destroySocket(socket);
    }

    //  Chooses least loaded I/O thread.
    protected final IOThread chooseIoThread(long affinity)
    {
        return ctx.chooseIoThread(affinity);
    }

    protected final void sendStop()
    {
        //  'stop' command goes always from administrative thread to
        //  the current object.
        Command cmd = new Command(this, Command.Type.STOP, tid);
        ctx.sendCommand(tid, cmd);
    }

    final void sendPlug(Own destination)
    {
        sendPlug(destination, true);
    }

    final void sendPlug(Own destination, boolean incSeqnum)
    {
        if (incSeqnum) {
            destination.incSeqnum();
        }

        Command cmd = new Command(destination, Command.Type.PLUG);
        sendCommand(cmd);
    }

    final void sendOwn(Own destination, Own object)
    {
        destination.incSeqnum();
        Command cmd = new Command(destination, Command.Type.OWN, object);
        sendCommand(cmd);
    }

    protected final void sendAttach(SessionBase destination, IEngine engine)
    {
        sendAttach(destination, engine, true);
    }

    protected final void sendAttach(SessionBase destination, IEngine engine, boolean incSeqnum)
    {
        if (incSeqnum) {
            destination.incSeqnum();
        }

        Command cmd = new Command(destination, Command.Type.ATTACH, engine);
        sendCommand(cmd);
    }

    protected final void sendBind(Own destination, Pipe pipe)
    {
        sendBind(destination, pipe, true);
    }

    protected final void sendBind(Own destination, Pipe pipe, boolean incSeqnum)
    {
        if (incSeqnum) {
            destination.incSeqnum();
        }

        Command cmd = new Command(destination, Command.Type.BIND, pipe);
        sendCommand(cmd);
    }

    protected final void sendActivateRead(Pipe destination)
    {
        Command cmd = new Command(destination, Command.Type.ACTIVATE_READ);
        sendCommand(cmd);
    }

    protected final void sendActivateWrite(Pipe destination, long msgsRead)
    {
        Command cmd = new Command(destination, Command.Type.ACTIVATE_WRITE, msgsRead);
        sendCommand(cmd);
    }

    protected final void sendHiccup(Pipe destination, YPipeBase<Msg> pipe)
    {
        Command cmd = new Command(destination, Command.Type.HICCUP, pipe);
        sendCommand(cmd);
    }

    protected final void sendPipeTerm(Pipe destination)
    {
        Command cmd = new Command(destination, Command.Type.PIPE_TERM);
        sendCommand(cmd);
    }

    protected final void sendPipeTermAck(Pipe destination, Pipe.State self)
    {
        Command cmd = new Command(destination, Command.Type.PIPE_TERM_ACK, self);
        sendCommand(cmd);
    }

    final void sendTermReq(Own destination, Own object)
    {
        Command cmd = new Command(destination, Command.Type.TERM_REQ, object);
        sendCommand(cmd);
    }

    final void sendTerm(Own destination, int linger)
    {
        Command cmd = new Command(destination, Command.Type.TERM, linger);
        sendCommand(cmd);
    }

    final void sendTermAck(Own destination)
    {
        Command cmd = new Command(destination, Command.Type.TERM_ACK, this);
        sendCommand(cmd);
    }

    protected final void sendReap(ZObject object)
    {
        Command cmd = new Command(ctx.getReaper(), Command.Type.REAP, object);
        sendCommand(cmd);
    }

    protected final void sendReaped(ZObject object)
    {
        Command cmd = new Command(ctx.getReaper(), Command.Type.REAPED, object);
        sendCommand(cmd);
    }

    final void sendInprocConnected(SocketBase socket)
    {
        Command cmd = new Command(socket, Command.Type.INPROC_CONNECTED);
        sendCommand(cmd);
    }

    final void sendDone()
    {
        Command cmd = new Command(null, Command.Type.DONE);
        ctx.sendCommand(Ctx.TERM_TID, cmd);
    }

    protected void processStop(int tid)
    {
        throw new UnsupportedOperationException();
    }

    protected void processPlug()
    {
        throw new UnsupportedOperationException();
    }

    protected void processOwn(Own object)
    {
        throw new UnsupportedOperationException();
    }

    protected void processAttach(IEngine engine)
    {
        throw new UnsupportedOperationException();
    }

    protected void processBind(Pipe pipe)
    {
        throw new UnsupportedOperationException();
    }

    protected void processActivateRead()
    {
        throw new UnsupportedOperationException();
    }

    protected void processActivateWrite(long msgsRead)
    {
        throw new UnsupportedOperationException();
    }

    protected void processHiccup(YPipeBase<Msg> hiccupPipe)
    {
        throw new UnsupportedOperationException();
    }

    protected void processPipeTerm()
    {
        throw new UnsupportedOperationException();
    }

    protected void processPipeTermAck(Pipe.State peerState)
    {
        throw new UnsupportedOperationException();
    }

    protected void processTermReq(Own object)
    {
        throw new UnsupportedOperationException();
    }

    protected void processTerm(int linger)
    {
        throw new UnsupportedOperationException();
    }

    protected void processTermAck(ZObject object)
    {
        throw new UnsupportedOperationException();
    }

    protected void processReap(ZObject object)
    {
        throw new UnsupportedOperationException();
    }

    protected void processReaped(ZObject object)
    {
        throw new UnsupportedOperationException();
    }

    //  Special handler called after a command that requires a seqnum
    //  was processed. The implementation should catch up with its counter
    //  of processed commands here.
    protected void processSeqnum()
    {
        throw new UnsupportedOperationException();
    }

    private void sendCommand(Command cmd)
    {
        ctx.sendCommand(cmd.destination.getTid(), cmd);
    }
}
