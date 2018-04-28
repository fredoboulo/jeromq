package zmq.io;

import java.nio.channels.SelectableChannel;
import java.util.HashSet;
import java.util.Set;

import zmq.Command;
import zmq.Ctx;
import zmq.Mailbox;
import zmq.Reaper;
import zmq.ZError;
import zmq.ZObject;
import zmq.poll.IPollEvents;
import zmq.poll.Poller;

public class IOThread extends ZObject implements IPollEvents
{
    //  I/O thread accesses incoming commands via this mailbox.
    private final Mailbox mailbox;

    //  Handle associated with mailbox' file descriptor.
    private final Poller.Handle mailboxHandle;

    //  I/O multiplexing is performed using a poller object.
    private final Poller poller;

    // I/O objects being plugged at the moment
    private final Set<IOObject> plugs = new HashSet<>();

    private final String name;

    // If true, the I/O thread is being reaped
    private boolean reaping;

    public IOThread(Ctx ctx, int tid)
    {
        super(ctx, tid);
        name = "iothread-" + tid;
        poller = new Poller(ctx, name);

        mailbox = new Mailbox(ctx, name, tid);
        SelectableChannel fd = mailbox.getFd();
        mailboxHandle = poller.addHandle(fd, this);
        poller.setPollIn(mailboxHandle);
    }

    public final void start()
    {
        poller.start();
    }

    private void destroy() throws ZError.IOException
    {
        poller.destroy();
        mailbox.destroy();
    }

    public final void stop()
    {
        sendStop();
    }

    public final Mailbox getMailbox()
    {
        return mailbox;
    }

    public final int getLoad()
    {
        return poller.getLoad();
    }

    @Override
    public final void inEvent()
    {
        //  TODO: Do we want to limit number of commands I/O thread can
        //  process in a single go?
        assert (!reaping);

        while (true) {
            //  Get the next command. If there is none, exit.
            Command cmd = mailbox.recv(0);
            if (cmd == null) {
                break;
            }

            //  Process the command.
            cmd.process();
            if (reaping) {
                break;
            }
        }
    }

    public final Poller getPoller(IOObject io)
    {
        boolean added = plugs.add(io);
        assert (added);
        return poller;
    }

    public final void givePoller(IOObject io)
    {
        plugs.remove(io);
        if (plugs.isEmpty() && reaping) {
            sendReaped(this);
        }
    }

    @Override
    protected final void processStop(int tid)
    {
        assert (tid == getTid());
        // we called ourselves

        reaping = true;
        poller.removeHandle(mailboxHandle);

        // transfer the ownership of this I/O thread to the reaper thread
        // who will take care of the rest of the shutdown process
        sendReap(this);
    }

    @Override
    public final void processReap(ZObject object)
    {
        assert (reaping);
        assert (object instanceof Reaper);

        Reaper reaper = (Reaper) object;
        if (plugs.isEmpty()) {
            reaper.processReaped(this);
        }
    }

    @Override
    public final void processReaped(ZObject object)
    {
        assert (reaping);
        assert (object instanceof Reaper);

        poller.stop();
        destroy();
    }

    @Override
    public String toString()
    {
        return name;
    }
}
