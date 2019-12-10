package zmq.pipe;

import zmq.Config;
import zmq.Msg;
import zmq.ZObject;
import zmq.util.Blob;

import java.util.ArrayList;
import java.util.List;

//  Note that pipe can be stored in three different arrays.
//  The array of inbound pipes (1), the array of outbound pipes (2) and
//  the generic array of pipes to deallocate (3).
public class Pipe extends ZObject
{
    public interface IPipeEvents
    {
        void readActivated(Pipe pipe);

        void writeActivated(Pipe pipe);

        void hiccuped(Pipe pipe);

        void pipeTerminated(Pipe pipe);
    }

    //  Underlying pipes for both directions.
    private YPipeBase<Msg> inpipe;
    private YPipeBase<Msg> outpipe;

    //  Can the pipe be read from / written to?
    private boolean inActive;
    private boolean outActive;

    //  High watermark for the outbound pipe.
    private int hwm;

    //  Low watermark for the inbound pipe.
    private int lwm;

    //  Number of messages read and written so far.
    private long msgsRead;
    private long msgsWritten;

    //  Last received peer's msgsRead. The actual number in the peer
    //  can be higher at the moment.
    private long peersMsgsRead;

    //  The pipe object on the other side of the pipepair.
    private Pipe peer;

    //  Sink to send events to.
    private IPipeEvents sink;

    //  States of the pipe endpoint:
    //  active: common state before any termination begins,
    //  delimiter_received: delimiter was read from pipe before
    //      term command was received,
    //  waiting_fo_delimiter: term command was already received
    //      from the peer but there are still pending messages to read,
    //  term_ack_sent: all pending messages were already read and
    //      all we are waiting for is ack from the peer,
    //  term_req_sent1: 'terminate' was explicitly called by the user,
    //  term_req_sent2: user called 'terminate' and then we've got
    //      term command from the peer as well.
    public enum State
    {
        ACTIVE,
        DELIMITER_RECEIVED,
        WAITING_FOR_DELIMITER,
        TERM_ACK_SENT,
        TERM_REQ_SENT_1,
        TERM_REQ_SENT_2
    }

    private State state;
    private final List<State> states = new ArrayList<>();

    //  If true, we receive all the pending inbound messages before
    //  terminating. If false, we terminate immediately when the peer
    //  asks us to.
    private boolean delay;

    //  Identity of the writer. Used uniquely by the reader side.
    private Blob identity;

    //  Pipe's credential.
    private Blob credential;

    private final boolean conflate;

    // JeroMQ only
    private final ZObject parent;

    //  Constructor is private. Pipe can only be created using
    //  pipepair function.
    private Pipe(ZObject parent, YPipeBase<Msg> inpipe, YPipeBase<Msg> outpipe, int inhwm, int outhwm, boolean conflate)
    {
        super(parent);
        this.inpipe = inpipe;
        this.outpipe = outpipe;
        inActive = true;
        outActive = true;
        hwm = outhwm;
        lwm = computeLwm(inhwm);
        msgsRead = 0;
        msgsWritten = 0;
        peersMsgsRead = 0;
        peer = null;
        sink = null;
        state(State.ACTIVE);
        this.delay = true;
        this.conflate = conflate;

        this.parent = parent;
    }

    //  This allows pipepair to create pipe objects.
    //  Create a pipepair for bi-directional transfer of messages.
    //  First HWM is for messages passed from first pipe to the second pipe.
    //  Second HWM is for messages passed from second pipe to the first pipe.
    //  Conflate specifies how the pipe behaves when the peer terminates. If true
    //  pipe receives all the pending messages before terminating, otherwise it
    //  terminates straight away.
    public static Pipe[] pair(ZObject[] parents, int[] hwms, boolean[] conflates)
    {
        Pipe[] pipes = new Pipe[2];
        //   Creates two pipe objects. These objects are connected by two ypipes,
        //   each to pass messages in one direction.

        YPipeBase<Msg> upipe1 = conflates[0] ? new YPipeConflate<>()
                : new YPipe<>(Config.MESSAGE_PIPE_GRANULARITY.getValue());
        YPipeBase<Msg> upipe2 = conflates[1] ? new YPipeConflate<>()
                : new YPipe<>(Config.MESSAGE_PIPE_GRANULARITY.getValue());

        pipes[0] = new Pipe(parents[0], upipe1, upipe2, hwms[1], hwms[0], conflates[0]);
        pipes[1] = new Pipe(parents[1], upipe2, upipe1, hwms[0], hwms[1], conflates[1]);

        pipes[0].setPeer(pipes[1]);
        pipes[1].setPeer(pipes[0]);

        return pipes;
    }

    private void state(State newState)
    {
        states.add(newState);
        this.state = newState;
    }

    //  Pipepair uses this function to let us know about
    //  the peer pipe object.
    private void setPeer(Pipe peer)
    {
        //  Peer can be set once only.
        assert (this.peer == null);
        assert (peer != null);
        this.peer = peer;
    }

    //  Specifies the object to send events to.
    public void setEventSink(IPipeEvents sink)
    {
        assert (this.sink == null);
        this.sink = sink;
    }

    //  Pipe endpoint can store an opaque ID to be used by its clients.
    public void setIdentity(Blob identity)
    {
        this.identity = identity;
    }

    public Blob getIdentity()
    {
        return identity;
    }

    public Blob getCredential()
    {
        return credential;
    }

    //  Returns true if there is at least one message to read in the pipe.
    public boolean checkRead()
    {
        if (!inActive) {
            return false;

        }
        if (state != State.ACTIVE && state != State.WAITING_FOR_DELIMITER) {
            return false;
        }

        //  Check if there's an item in the pipe.
        if (!inpipe.checkRead()) {
            inActive = false;
            return false;
        }

        //  If the next item in the pipe is message delimiter,
        //  initiate termination process.
        if (isDelimiter(inpipe.probe())) {
            Msg msg = inpipe.read();
            assert (msg != null);
            processDelimiter();
            return false;
        }

        return true;
    }

    //  Reads a message to the underlying pipe.
    public Msg read()
    {
        if (!inActive) {
            return null;
        }
        if (state != State.ACTIVE && state != State.WAITING_FOR_DELIMITER) {
            return null;
        }

        while (true) {
            Msg msg = inpipe.read();

            if (msg == null) {
                inActive = false;
                return null;
            }

            //  If this is a credential, save a copy and receive next message.
            if (msg.isCredential()) {
                credential = Blob.createBlob(msg);
                continue;
            }

            //  If delimiter was read, start termination process of the pipe.
            if (msg.isDelimiter()) {
                processDelimiter();
                return null;
            }

            if (!msg.hasMore() && !msg.isIdentity()) {
                msgsRead++;
            }

            if (lwm > 0 && msgsRead % lwm == 0) {
                sendActivateWrite(peer, msgsRead);
            }
            return msg;
        }
    }

    //  Checks whether messages can be written to the pipe. If writing
    //  the message would cause high watermark the function returns false.
    public boolean checkWrite()
    {
        if (!outActive || state != State.ACTIVE) {
            return false;
        }

        // TODO DIFF V4 small change, it is done like this in 4.2.2
        boolean full = !checkHwm();

        if (full) {
            outActive = false;
            return false;
        }

        return true;
    }

    //  Writes a message to the underlying pipe. Returns false if the
    //  message cannot be written because high watermark was reached.
    public boolean write(Msg msg)
    {
        if (!checkWrite()) {
            return false;
        }

        boolean more = msg.hasMore();
        boolean identity = msg.isIdentity();
        outpipe.write(msg, more);

        if (!more && !identity) {
            msgsWritten++;
        }

        return true;
    }

    //  Remove unfinished parts of the outbound message from the pipe.
    public void rollback()
    {
        //  Remove incomplete message from the outbound pipe.
        Msg msg;
        if (outpipe != null) {
            while ((msg = outpipe.unwrite()) != null) {
                assert (msg.hasMore());
            }
        }
    }

    //  Flush the messages downstream.
    public void flush()
    {
        //  The peer does not exist anymore at this point.
        if (state == State.TERM_ACK_SENT) {
            return;
        }

        if (outpipe != null && !outpipe.flush()) {
            sendActivateRead(peer);
        }
    }

    @Override
    protected void processActivateRead()
    {
        if (!inActive && (state == State.ACTIVE || state == State.WAITING_FOR_DELIMITER)) {
            inActive = true;
            try {
                sink.readActivated(this);
            }
            catch (IndexOutOfBoundsException e) {
                System.out.println(state);
                e.printStackTrace();
            }
        }
    }

    @Override
    protected void processActivateWrite(long msgsRead)
    {
        //  Remember the peers's message sequence number.
        peersMsgsRead = msgsRead;

        if (!outActive && state == State.ACTIVE) {
            outActive = true;
            sink.writeActivated(this);
        }
    }

    @Override
    protected void processHiccup(YPipeBase<Msg> pipe)
    {
        //  Destroy old outpipe. Note that the read end of the pipe was already
        //  migrated to this thread.
        assert (outpipe != null);
        outpipe.flush();
        Msg msg;
        while ((msg = outpipe.read()) != null) {
            if (!msg.hasMore()) {
                msgsWritten--;
            }
        }

        //  Plug in the new outpipe.
        assert (pipe != null);
        outpipe = pipe;
        outActive = true;

        //  If appropriate, notify the user about the hiccup.
        if (state == State.ACTIVE) {
            sink.hiccuped(this);
        }
    }

    @Override
    protected void processPipeTerm()
    {
        runEvent(Event.PipeTerm, null);
    }

    @Override
    protected void processPipeTermAck(State peerState)
    {
        assert (state == State.TERM_ACK_SENT || state == State.TERM_REQ_SENT_1
                || state == State.TERM_REQ_SENT_2) : state;
        assert (peerState == State.TERM_ACK_SENT || peerState == State.TERM_REQ_SENT_1
                || peerState == State.TERM_REQ_SENT_2) : peerState;

        //  Notify the user that all the references to the pipe should be dropped.
        assert (sink != null);
        sink.pipeTerminated(this);
        runEvent(Event.PipeTermAck, peerState);
    }

    private void closeInbound()
    {
        // If the inbound pipe has already been deallocated, then we're done.
        if (inpipe == null) {
            return;
        }

        //  We'll deallocate the inbound pipe, the peer will deallocate the outbound
        //  pipe (which is an inbound pipe from its point of view).
        //  First, delete all the unread messages in the pipe. We have to do it by
        //  hand because msg_t doesn't have automatic destructor. Then deallocate
        //  the ypipe itself.
        if (!conflate) {
            while (inpipe.read() != null) {
                // do nothing
            }
        }

        //  Deallocate the pipe object
        inpipe = null;
    }

    public void setNoDelay()
    {
        this.delay = false;
    }

    //  Ask pipe to terminate. The termination will happen asynchronously
    //  and user will be notified about actual deallocation by 'terminated'
    //  event. If delay is true, the pending messages will be processed
    //  before actual shutdown.
    public void terminate(boolean delay)
    {
        //  Overload the value specified at pipe creation.
        this.delay = delay;

        runEvent(Event.Terminate, null);
    }

    private void writeDelimiter()
    {
        //  Stop outbound flow of messages.
        outActive = false;

        if (outpipe != null) {
            //  Drop any unfinished outbound messages.
            rollback();

            //  Write the delimiter into the pipe. Note that watermarks are not
            //  checked; thus the delimiter can be written even when the pipe is full.

            Msg msg = new Msg();
            msg.initDelimiter();
            outpipe.write(msg, false);
            flush();
        }
    }

    private enum Event {
        Terminate, Delimiter, PipeTerm, PipeTermAck
    }

    private void runEvent(Event event, State peerState)
    {
        switch (state) {
        case ACTIVE:
            switch (event) {
            case Terminate:
                // The simple sync termination case. Ask the peer to terminate and wait
                // for the ack.
                state(State.TERM_REQ_SENT_1);
                writeDelimiter();
                sendPipeTerm(peer);
                break;
            case PipeTerm:
                // This is the simple case of peer-induced termination. If there are no
                // more pending messages to read, or if the pipe was configured to drop
                // pending messages, we can move directly to the term_ack_sent state.
                // Otherwise we'll hang up in waiting_for_delimiter state till all
                // pending messages are read.
                if (delay) {
                    state(State.WAITING_FOR_DELIMITER);
                }
                else {
                    state(State.TERM_ACK_SENT);
                    outpipe = null;
                    sendPipeTermAck(peer, state);
                }
                break;
            case Delimiter:
                state(State.DELIMITER_RECEIVED);
                break;
            default:
                assert (false) : event + " " + state;
                break;
            }
            break;
        case WAITING_FOR_DELIMITER:
            switch (event) {
            case Terminate:
                writeDelimiter();
                if (!delay) {
                    // There are still pending messages available, but the user calls
                    // 'terminate'. We can act as if all the pending messages were read.
                    outpipe = null;
                    state(State.TERM_ACK_SENT);
                    sendPipeTermAck(peer, state);
                }
                // If there are pending messages still available, do nothing.
                break;
            case Delimiter:
                outpipe = null;
                state(State.TERM_ACK_SENT);
                sendPipeTermAck(peer, state);
                break;
            default:
                assert (false) : event + " " + state;
                break;
            }
            break;
        case DELIMITER_RECEIVED:
            switch (event) {
            case Terminate:
                // We've already got delimiter, but not term command yet. We can ignore
                // the delimiter and ack synchronously terminate as if we were in
                // active state.
                state(State.TERM_REQ_SENT_1);
                writeDelimiter();
                sendPipeTerm(peer);
                break;
            case PipeTerm:
                // Delimiter happened to arrive before the term command. Now we have the
                // term command as well, so we can move straight to term_ack_sent state.
                state(State.TERM_ACK_SENT);
                outpipe = null;
                sendPipeTermAck(peer, state);
                break;
            default:
                assert (false) : event + " " + state;
                break;
            }
            break;
        case TERM_REQ_SENT_1:
            switch (event) {
            case Terminate:
                // If terminate was already called, we can ignore the duplicit invocation.
                return;
            case PipeTerm:
                // This is the case where both ends of the pipe are closed in parallel.
                // We simply reply to the request by ack and continue waiting for our
                // own ack.
                state(State.TERM_REQ_SENT_2);
                outpipe = null;
                sendPipeTermAck(peer, state);
                break;
            case PipeTermAck:
                outpipe = null;
                if (peerState == State.TERM_ACK_SENT) {
                    // we have to ack the peer before deallocating this side of the pipe.
                    sendPipeTermAck(peer, state);
                }
                // in other cases, no need to send another ack
                closeInbound();
                break;
            default:
                assert (false) : event + " " + state;
                break;
            }
            break;
        case TERM_REQ_SENT_2:
            switch (event) {
            case Terminate:
                // If terminate was already called, we can ignore the duplicit invocation.
                return;
            case PipeTermAck:
                // nothing to do
                assert (peerState == State.TERM_REQ_SENT_2 || peerState == State.TERM_ACK_SENT) : peerState;
                closeInbound();
                break;
            default:
                System.out.println();
                System.out.println("Run Event " + event + " > state: " + state + " peer: " + peerState);
                System.out.println();
                break;
            }
            break;
        case TERM_ACK_SENT:
            switch (event) {
            case Terminate:
                // If the pipe is in the final phase of async termination, it's going to
                // closed anyway. No need to do anything special here.
                return;
            case PipeTermAck:
                assert (peerState == State.TERM_REQ_SENT_1 | peerState == State.TERM_REQ_SENT_2) : peerState;
                // Simply deallocate the pipe.
                closeInbound();
                break;
            default:
                System.out.println();
                System.out.println("Run Event " + event + " > state: " + state + " peer: " + peerState);
                System.out.println();
                break;
            }
            break;
        default:
            break;
        }
    }

    //  Returns true if the message is delimiter; false otherwise.
    private static boolean isDelimiter(Msg msg)
    {
        return msg.isDelimiter();
    }

    //  Computes appropriate low watermark from the given high watermark.
    private static int computeLwm(int hwm)
    {
        //  Compute the low water mark. Following point should be taken
        //  into consideration:
        //
        //  1. LWM has to be less than HWM.
        //  2. LWM cannot be set to very low value (such as zero) as after filling
        //     the queue it would start to refill only after all the messages are
        //     read from it and thus unnecessarily hold the progress back.
        //  3. LWM cannot be set to very high value (such as HWM-1) as it would
        //     result in lock-step filling of the queue - if a single message is
        //     read from a full queue, writer thread is resumed to write exactly one
        //     message to the queue and go back to sleep immediately. This would
        //     result in low performance.
        //
        //  Given the 3. it would be good to keep HWM and LWM as far apart as
        //  possible to reduce the thread switching overhead to almost zero,
        //  say HWM-LWM should be max_wm_delta.
        //
        //  That done, we still we have to account for the cases where
        //  HWM < max_wm_delta thus driving LWM to negative numbers.
        //  Let's make LWM 1/2 of HWM in such cases.
        //        return (hwm +1) /2;
        return (hwm + 1) / 2;
    }

    //  Handler for delimiter read from the pipe.
    private void processDelimiter()
    {
        assert (state == State.ACTIVE || state == State.WAITING_FOR_DELIMITER);
        runEvent(Event.Delimiter, null);
    }

    //  Temporarily disconnects the inbound message stream and drops
    //  all the messages on the fly. Causes 'hiccuped' event to be generated
    //  in the peer.
    public void hiccup()
    {
        //  If termination is already under way do nothing.
        if (state != State.ACTIVE) {
            return;
        }

        //  We'll drop the pointer to the inpipe. From now on, the peer is
        //  responsible for deallocating it.
        inpipe = null;

        //  Create new inpipe.
        if (conflate) {
            inpipe = new YPipeConflate<>();
        }
        else {
            inpipe = new YPipe<>(Config.MESSAGE_PIPE_GRANULARITY.getValue());
        }
        inActive = true;

        //  Notify the peer about the hiccup.
        sendHiccup(peer, inpipe);
    }

    public void setHwms(int inhwm, int outhwm)
    {
        lwm = computeLwm(inhwm);
        hwm = outhwm;
    }

    public boolean checkHwm()
    {
        // TODO DIFF V4 small change, it is done like this in 4.2.2
        boolean full = hwm > 0 && (msgsWritten - peersMsgsRead) >= hwm;
        return !full;
    }

    @Override
    public String toString()
    {
        return super.toString() + "(" + parent.getClass().getSimpleName() + "[" + parent.getTid() + "]->"
                + peer.parent.getClass().getSimpleName() + "[" + peer.parent.getTid() + "])";
    }
}
