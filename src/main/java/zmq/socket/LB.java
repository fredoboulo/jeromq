package zmq.socket;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import zmq.Msg;
import zmq.ZError;
import zmq.pipe.Pipe;
import zmq.util.Errno;
import zmq.util.ValueReference;

public class LB
{
    //  List of outbound pipes.
    private final List<Pipe> pipes;

    //  Number of active pipes. All the active pipes are located at the
    //  beginning of the pipes array.
    private int active;

    //  Points to the last pipe that the most recent message was sent to.
    private int current;

    //  True if last we are in the middle of a multipart message.
    private boolean more;

    //  True if we are dropping current message.
    private boolean dropping;

    public LB()
    {
        active = 0;
        current = 0;
        more = false;
        dropping = false;

        pipes = new ArrayList<>();
    }

    public void attach(Pipe pipe)
    {
        pipes.add(pipe);
        activated(pipe);
    }

    public void terminated(Pipe pipe)
    {
        final int index = pipes.indexOf(pipe);

        //  If we are in the middle of multipart message and current pipe
        //  have disconnected, we have to drop the remainder of the message.
        if (index == current && more) {
            dropping = true;
        }

        //  Remove the pipe from the list; adjust number of active pipes
        //  accordingly.
        if (index < active) {
            active--;
            Collections.swap(pipes, index, active);
            if (current == active) {
                current = 0;
            }
        }
        pipes.remove(pipe);
    }

    public void activated(Pipe pipe)
    {
        int index = pipes.indexOf(pipe);
        if (active < pipes.size()) {
            //  Move the pipe to the list of active pipes.
            Collections.swap(pipes, index, active);
            active++;
        }
    }

    public boolean sendpipe(Msg msg, Errno errno, ValueReference<Pipe> pipe)
    {
        //  Drop the message if required. If we are at the end of the message
        //  switch back to non-dropping mode.
        if (dropping) {
            more = msg.hasMore();
            dropping = more;

            // msg_.close();
            return true;
        }

        while (active > 0) {
            if (pipes.get(current).write(msg)) {
                if (pipe != null) {
                    pipe.set(pipes.get(current));
                }
                break;
            }

            // If send fails for multi-part msg rollback other
            // parts sent earlier and return EAGAIN.
            // Application should handle this as suitable
            if (more) {
                pipes.get(current).rollback();
                // At this point the pipe is already being deallocated
                // and the first N frames are unreachable (_outpipe is
                // most likely already NULL so rollback won't actually do
                // anything and they can't be un-written to deliver later).
                // Return EFAULT to socket_base caller to drop current message
                // and any other subsequent frames to avoid them being
                // "stuck" and received when a new client reconnects, which
                // would break atomicity of multi-part messages (in blocking mode
                // socket_base just tries again and again to send the same message)
                // Note that given dropping mode returns 0, the user will
                // never know that the message could not be delivered, but
                // can't really fix it without breaking backward compatibility.
                // -2/EAGAIN will make sure socket_base caller does not re-enter
                // immediately or after a short sleep in blocking mode.
                dropping = msg.hasMore();
                more = false;
                errno.set(ZError.EAGAIN);
                return false;
            }
            active--;
            if (current < active) {
                Collections.swap(pipes, current, active);
            }
            else {
                current = 0;
            }
        }

        //  If there are no pipes we cannot send the message.
        if (active == 0) {
            errno.set(ZError.EAGAIN);
            return false;
        }

        //  If it's final part of the message we can flush it downstream and
        //  continue round-robining (load balance).
        more = msg.hasMore();
        if (!more) {
            pipes.get(current).flush();
            if (++current >= active) {
                current = 0;
            }
        }

        return true;
    }

    public boolean hasOut()
    {
        //  If one part of the message was already written we can definitely
        //  write the rest of the message.
        if (more) {
            return true;
        }

        while (active > 0) {
            //  Check whether a pipe has room for another message.
            if (pipes.get(current).checkWrite()) {
                return true;
            }

            //  Deactivate the pipe.
            active--;
            Collections.swap(pipes, current, active);
            if (current == active) {
                current = 0;
            }
        }

        return false;
    }
}
