package zmq;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import zmq.io.IOThread;
import zmq.poll.IPollEvents;
import zmq.poll.Poller;

public final class Reaper extends ZObject implements IPollEvents
{
    //  Reaper thread accesses incoming commands via this mailbox.
    private final Mailbox mailbox;

    //  Handle associated with mailbox' file descriptor.
    private final Poller.Handle mailboxHandle;

    //  I/O multiplexing is performed using a poller object.
    final Poller poller;

    //  Sockets being reaped at the moment.
    private final Set<SocketBase> sockets = new HashSet<>();
    //  I/O threads being reaped at the moment.
    private final Set<IOThread> pendingIOs = new HashSet<>();

    //  If true, we are in the process of reaping I/O threads.
    private final AtomicBoolean ioReaping = new AtomicBoolean();

    //  If true, we were already asked to terminate.
    private final AtomicBoolean terminating = new AtomicBoolean();

    private final String name;

    //  I/O threads asked to reap by the context.
    private List<IOThread> ioThreads;

    Reaper(Ctx ctx, int tid)
    {
        super(ctx, tid);
        name = "reaper-" + tid;
        poller = new Poller(ctx, name);

        mailbox = new Mailbox(ctx, name, tid);

        SelectableChannel fd = mailbox.getFd();
        mailboxHandle = poller.addHandle(fd, this);
        poller.setPollIn(mailboxHandle);
    }

    void destroy() throws IOException
    {
        poller.destroy();
        mailbox.destroy();
    }

    Mailbox getMailbox()
    {
        return mailbox;
    }

    void start()
    {
        poller.start();
    }

    // stops the reaper with the I/O threads in charge
    public void stop(List<IOThread> ioThreads)
    {
        if (this.ioThreads == null) {
            this.ioThreads = ioThreads;
        }
        if (!terminating.get()) {
            sendStop();
        }
    }

    @Override
    public void inEvent()
    {
        while (true) {
            //  Get the next command. If there is none, exit.
            Command cmd = mailbox.recv(0);
            if (cmd == null) {
                break;
            }

            //  Process the command.
            cmd.process();
        }
        // check if we can be destroyed at the end of every command
        checkDestroy();
    }

    @Override
    protected void processStop(int tid)
    {
        assert (getTid() == tid) : getTid() + " <> " + tid;
        if (terminating.compareAndSet(false, true)) {
            //  If there are no sockets being reaped finish immediately.
            stopIO();
        }
    }

    @Override
    protected void processReap(ZObject object)
    {
        if (object instanceof SocketBase) {
            SocketBase socket = (SocketBase) object;
            // the socket pass into reaper's control.
            // it will be released only when it tells it has been reaped.
            sockets.add(socket);

            //  Add the socket to the poller.
            socket.processReap(this);
        }
        if (object instanceof IOThread) {
            // time to reap IO thread
            assert (sockets.isEmpty());
            assert (ioThreads != null);
            assert (pendingIOs.contains(object));

            IOThread io = (IOThread) object;
            io.processReap(this);
        }
    }

    @Override
    public void processReaped(ZObject object)
    {
        if (sockets.remove(object)) {
            SocketBase socket = (SocketBase) object;

            // the socket is notified that its reaping has been taken into account.
            socket.processReaped(socket);
            // the context is notified that the socket is totally deallocated.
            ctx.reaped(socket);
        }
        if (terminating.get()) {
            stopIO();
        }
        if (pendingIOs.remove(object)) {
            assert (sockets.isEmpty());
            assert (ioThreads != null);

            IOThread io = (IOThread) object;
            // I/O thread is notified that its reaping has been taken into account.
            io.processReaped(this);
        }
        checkDestroy();
    }

    private void stopIO()
    {
        assert (ioThreads != null);
        if (sockets.isEmpty() && ioReaping.compareAndSet(false, true)) {
            pendingIOs.addAll(ioThreads);
            for (IOThread io : ioThreads) {
                io.stop();
            }
        }
    }

    private void checkDestroy()
    {
        //  If reaped was already asked to terminate and there are no more sockets,
        //  finish immediately.
        if (sockets.isEmpty() && terminating.get() && ioReaping.get() && pendingIOs.isEmpty()) {
            assert (ioThreads != null);
            sendDone();
            poller.removeHandle(mailboxHandle);
            poller.stop();
        }
    }
}
