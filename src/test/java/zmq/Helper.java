package zmq;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicInteger;

import zmq.util.Errno;

public class Helper
{
    public static AtomicInteger counter = new AtomicInteger(2);

    private Helper()
    {
    }

    public static class DummyCtx extends Ctx
    {
    }

    public static class DummySocketChannel implements WritableByteChannel
    {
        private int    bufsize;
        private byte[] buf;

        public DummySocketChannel()
        {
            this(64);
        }

        public DummySocketChannel(int bufsize)
        {
            this.bufsize = bufsize;
            buf = new byte[bufsize];
        }

        public byte[] data()
        {
            return buf;
        }

        @Override
        public void close() throws IOException
        {
        }

        @Override
        public boolean isOpen()
        {
            return true;
        }

        @Override
        public int write(ByteBuffer src) throws IOException
        {
            int remaining = src.remaining();
            if (remaining > bufsize) {
                src.get(buf);
                return bufsize;
            }
            src.get(buf, 0, remaining);
            return remaining;
        }

    }

    public static void bounce(SocketBase sb, SocketBase sc)
    {
        byte[] content = "12345678ABCDEFGH12345678abcdefgh".getBytes(ZMQ.CHARSET);

        //  Send the message.
        int rc = ZMQ.send(sc, content, 32, ZMQ.ZMQ_SNDMORE);
        assert (rc == 32);
        rc = ZMQ.send(sc, content, 32, 0);
        assertThat(rc, is(32));

        //  Bounce the message back.
        Msg msg;
        msg = ZMQ.recv(sb, 0);
        assert (msg.size() == 32);
        long rcvmore = ZMQ.getSocketOption(sb, ZMQ.ZMQ_RCVMORE);
        assert (rcvmore == 1);
        msg = ZMQ.recv(sb, 0);
        assert (rc == 32);
        rcvmore = ZMQ.getSocketOption(sb, ZMQ.ZMQ_RCVMORE);
        assert (rcvmore == 0);
        rc = ZMQ.send(sb, new Msg(msg), ZMQ.ZMQ_SNDMORE);
        assert (rc == 32);
        rc = ZMQ.send(sb, new Msg(msg), 0);
        assert (rc == 32);

        //  Receive the bounced message.
        msg = ZMQ.recv(sc, 0);
        assert (rc == 32);
        rcvmore = ZMQ.getSocketOption(sc, ZMQ.ZMQ_RCVMORE);
        assertThat(rcvmore, is(1L));
        msg = ZMQ.recv(sc, 0);
        assert (rc == 32);
        rcvmore = ZMQ.getSocketOption(sc, ZMQ.ZMQ_RCVMORE);
        assertThat(rcvmore, is(0L));
        //  Check whether the message is still the same.
        //assert (memcmp (buf2, content, 32) == 0);
    }

    public static void expectBounceFail(SocketBase server, SocketBase client)
    {
        final byte[] content = "12345678ABCDEFGH12345678abcdefgh".getBytes(ZMQ.CHARSET);
        final int timeout = 250;
        final Errno errno = new Errno();

        //  Send message from client to server
        ZMQ.setSocketOption(client, ZMQ.ZMQ_SNDTIMEO, timeout);

        int rc = ZMQ.send(client, content, 32, ZMQ.ZMQ_SNDMORE);
        assert ((rc == 32) || ((rc == -1) && errno.is(ZError.EAGAIN)));
        rc = ZMQ.send(client, content, 32, 0);
        assert ((rc == 32) || ((rc == -1) && errno.is(ZError.EAGAIN)));

        //  Receive message at server side (should not succeed)
        ZMQ.setSocketOption(server, ZMQ.ZMQ_RCVTIMEO, timeout);

        Msg msg = ZMQ.recv(server, 0);
        assert (msg == null);
        assert errno.is(ZError.EAGAIN);

        //  Send message from server to client to test other direction
        //  If connection failed, send may block, without a timeout
        ZMQ.setSocketOption(server, ZMQ.ZMQ_SNDTIMEO, timeout);

        rc = ZMQ.send(server, content, 32, ZMQ.ZMQ_SNDMORE);
        assert (rc == 32 || ((rc == -1) && errno.is(ZError.EAGAIN)));
        rc = ZMQ.send(server, content, 32, 0);
        assert (rc == 32 || ((rc == -1) && errno.is(ZError.EAGAIN)));

        //  Receive message at client side (should not succeed)
        ZMQ.setSocketOption(client, ZMQ.ZMQ_RCVTIMEO, timeout);
        msg = ZMQ.recv(client, 0);
        assert (msg == null);
        assert errno.is(ZError.EAGAIN);
    }

    public static int send(SocketBase socket, String data)
    {
        return ZMQ.send(socket, data, 0);
    }

    public static int sendMore(SocketBase socket, String data)
    {
        return ZMQ.send(socket, data, ZMQ.ZMQ_SNDMORE);
    }

    public static String recv(SocketBase socket)
    {
        Msg msg = ZMQ.recv(socket, 0);
        assert (msg != null);
        return new String(msg.data(), ZMQ.CHARSET);
    }

    //  Sends a message composed of frames that are C strings or null frames.
    //  The list must be terminated by SEQ_END.
    //  Example: s_send_seq (req, "ABC", 0, "DEF", SEQ_END);
    public static void sendSeq(SocketBase socket, String... data)
    {
        int rc = 0;
        for (int idx = 0; idx < data.length - 1; ++idx) {
            rc = sendMore(socket, data[idx]);
            assert (rc == data[idx].length());
        }
        rc = send(socket, data[data.length - 1]);
        assert (rc == data[data.length - 1].length());
    }

    //  Receives message a number of frames long and checks that the frames have
    //  the given data which can be either C strings or 0 for a null frame.
    //  The list must be terminated by SEQ_END.
    //  Example: s_recv_seq (rep, "ABC", 0, "DEF", SEQ_END);
    public static void recvSeq(SocketBase socket, String... data)
    {
        String rc;
        for (int idx = 0; idx < data.length; ++idx) {
            rc = recv(socket);
            assert (data[idx].equals(rc));
        }
    }

    public static void send(Socket sa, String data) throws IOException
    {
        byte[] content = data.getBytes(ZMQ.CHARSET);

        byte[] length = String.format("%04d", content.length).getBytes(ZMQ.CHARSET);

        byte[] buf = new byte[1024];
        int reslen;
        int rc;
        //  Bounce the message back.
        InputStream in = sa.getInputStream();
        OutputStream out = sa.getOutputStream();

        out.write(length);
        out.write(content);

        System.out.println("sent " + data.length() + " " + data);
        int toRead = 4; // 4 + greeting_size
        int read = 0;
        while (toRead > 0) {
            rc = in.read(buf, read, toRead);
            read += rc;
            toRead -= rc;
            System.out.println("read " + rc + " total_read " + read + " toRead " + toRead);
        }
        System.out.println(String.format("%02x %02x %02x %02x", buf[0], buf[1], buf[2], buf[3]));
        try {
            Thread.sleep(1000);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        reslen = Integer.valueOf(new String(buf, 0, 4, ZMQ.CHARSET));

        in.read(buf, 0, reslen);
        System.out.println("recv " + reslen + " " + new String(buf, 0, reslen, ZMQ.CHARSET));
    }

    /**
     * Repeat count times the string in input.
     * @param count the number of repeats
     * @param string the string to repeat
     * @return the repeated string
     */
    public static String repeat(int count, String string)
    {
        return count < 1 ? "" : String.format(String.format("%%%ds", count), " ").replace(" ", string);
    }

    /**
     * Provides a string able to erase count characters on the console.
     * @param count the number of characters to erase.
     * @return the erasing string.
     */
    public static String erase(int count)
    {
        return repeat(count, "\b \b");
    }

    /**
     * Provides a string able to rewind to count characters on the console.
     * @param count the number of characters to rewind.
     * @return the rewinding string.
     */
    public static String rewind(int count)
    {
        return repeat(count, "\b");
    }

    /**
     * Provides a string able to erase a string on the console.
     * @param string the string to erase.
     * @return the erasing string.
     */
    public static String erase(String string)
    {
        return erase(string.length());
    }

    /**
     * Provides a string able to rewind to a string on the console.
     * @param count the number of characters to rewind.
     * @return the rewinding string.
     */
    public static String rewind(String string)
    {
        return rewind(string.length());
    }

    public static String toString(int... zmq)
    {
        if (zmq.length == 0) {
            return "[]";
        }

        StringBuilder builder = new StringBuilder();
        builder.append("[");
        for (int s : zmq) {
            String string = toString(s);
            builder.append(string);
            builder.append(", ");
        }
        builder.append("]");
        return builder.toString();
    }

    public static String toString(int zmq)
    {
        String string = Integer.toString(zmq);
        switch (zmq) {
        case ZMQ.ZMQ_DEALER:
            string = "DEALER";
            break;
        case ZMQ.ZMQ_ROUTER:
            string = "ROUTER";
            break;
        case ZMQ.ZMQ_REQ:
            string = "REQ";
            break;
        case ZMQ.ZMQ_REP:
            string = "REP";
            break;
        case ZMQ.ZMQ_PAIR:
            string = "PAIR";
            break;
        case ZMQ.ZMQ_PUB:
            string = "PUB";
            break;
        case ZMQ.ZMQ_SUB:
            string = "SUB";
            break;
        case ZMQ.ZMQ_PUSH:
            string = "PUSH";
            break;
        case ZMQ.ZMQ_PULL:
            string = "PULL";
            break;
        case ZMQ.ZMQ_DECODER:
            string = "DECODER";
            break;
        case ZMQ.ZMQ_ENCODER:
            string = "ENCODER";
            break;
        case ZMQ.ZMQ_XPUB:
            string = "XPUB";
            break;
        case ZMQ.ZMQ_XSUB:
            string = "XSUB";
            break;

        default:
            break;
        }
        return string;
    }
}
