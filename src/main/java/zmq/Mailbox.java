package zmq;

import java.nio.channels.SelectableChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import zmq.pipe.YPipe;
import zmq.util.Errno;

public final class Mailbox
{
    //  The pipe to store actual commands.
    private final YPipe<Command> cpipe;

    //  Signaler to pass signals from writer thread to reader thread.
    private final Signaler signaler;

    //  There are two threads receiving from the mailbox (applicative and reaper), but there
    //  is arbitrary number of threads sending. Given that ypipe requires
    //  synchronized access on both of its endpoints, we have to synchronize
    //  the sending side.
    private final Lock writeSync;
    private final Lock readSync;

    //  True if the underlying pipe is active, i.e. when we are allowed to
    //  read commands from it.
    private boolean active;

    // mailbox name, for better debugging
    private final String name;

    private final Errno errno;

    public Mailbox(Ctx ctx, String name, int id)
    {
        this.errno = ctx.errno();
        cpipe = new YPipe<>(Config.COMMAND_PIPE_GRANULARITY.getValue());
        writeSync = new ReentrantLock(false);
        readSync = new ReentrantLock(false);
        signaler = new Signaler(ctx, name, errno);

        //  Get the pipe into passive state. That way, if the users starts by
        //  polling on the associated file descriptor it will get woken up when
        //  new command is posted.

        Command cmd = cpipe.read();
        assert (cmd == null);
        active = false;

        this.name = name;
    }

    public SelectableChannel getFd()
    {
        return signaler.getFd();
    }

    void send(final Command cmd)
    {
        boolean ok = false;
        writeSync.lock();
        try {
            cpipe.write(cmd, false);
            ok = cpipe.flush();
        }
        finally {
            writeSync.unlock();
        }

        if (!ok) {
            signaler.send();
        }
    }

    public Command recv(long timeout)
    {
        readSync.lock();
        try {
            // we have to protect all the private members
            return recvLocked(timeout);
        }
        finally {
            readSync.unlock();
        }
    }

    private Command recvLocked(long timeout)
    {
        Command cmd;
        //  Try to get the command straight away.
        if (active) {
            cmd = cpipe.read();
            if (cmd != null) {
                return cmd;
            }

            //  If there are no more commands available, switch into passive state.
            active = false;
        }

        //  Wait for signal from the command sender.
        boolean rc = signaler.waitEvent(timeout);
        if (!rc) {
            assert (errno.get() == ZError.EAGAIN || errno.get() == ZError.EINTR) : errno.get();
            return null;
        }

        //  Receive the signal.
        signaler.recv();
        if (errno.get() == ZError.EINTR) {
            return null;
        }

        //  Switch into active state.
        active = true;

        //  Get a command.
        cmd = cpipe.read();
        assert (cmd != null) : "command shall never be null when read";

        return cmd;
    }

    public void destroy() throws ZError.IOException
    {
        // Work around problem that other threads might still be in our
        // send() method, by waiting on the mutex before disappearing.
        writeSync.lock();
        writeSync.unlock();

        // wait to have the read lock
        readSync.lock();
        // got it
        try {
            active = false;
            signaler.destroy();

            //  Retrieve and deallocate commands inside the cpipe.
            Command cmd = cpipe.read();
            while (cmd != null) {
                Command.Type type = cmd.type;
                // it happens sometimes on Ctx.terminate -> SocketBase.stop
                // or on the terminater
                assert (type == Command.Type.DONE || type == Command.Type.STOP) : type;
                cmd = cpipe.read();
            }
        }
        finally {
            readSync.unlock();
        }
    }

    @Override
    public String toString()
    {
        return super.toString() + "[" + name + "]";
    }
}
