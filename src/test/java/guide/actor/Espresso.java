package guide.actor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.zeromq.ZActor;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;
import org.zeromq.ZProxy;
import org.zeromq.ZProxy.Plug;

//  Espresso Pattern
//  This shows how to capture data using a pub-sub proxy
public class Espresso
{
    //  The subscriber thread requests messages starting with
    //  A and B, then reads and counts incoming messages.
    private static class Subscriber extends ZActor.SimpleActor
    {
        private int count;

        @Override
        public List<Socket> createSockets(ZContext ctx, Object... args)
        {
            return Collections.singletonList(ctx.createSocket(ZMQ.SUB));
        }

        @Override
        public void start(Socket pipe, List<Socket> sockets, ZPoller poller)
        {
            Socket subscriber = sockets.get(0);
            subscriber.connect("tcp://localhost:6001");
            subscriber.subscribe("A");
            subscriber.subscribe("B".getBytes(ZMQ.CHARSET));
            poller.register(subscriber, ZPoller.IN);
        }

        @Override
        public boolean stage(Socket socket, Socket pipe, ZPoller poller, int events)
        {
            String string = socket.recvStr();
            return string == null || count++ < 5;
        }
    }

    //  .split publisher thread
    //  The publisher sends random messages starting with A-J:
    private static class Publisher extends ZActor.SimpleActor
    {
        private final Random rand = new Random(System.currentTimeMillis());
        private int          count;

        @Override
        public List<Socket> createSockets(ZContext ctx, Object... args)
        {
            return Collections.singletonList(ctx.createSocket(ZMQ.PUB));
        }

        @Override
        public void start(Socket pipe, List<Socket> sockets, ZPoller poller)
        {
            Socket publisher = sockets.get(0);
            publisher.bind("tcp://*:6000");
            poller.register(publisher, ZPoller.OUT);
        }

        @Override
        public boolean stage(Socket socket, Socket pipe, ZPoller poller, int events)
        {
            ZMQ.msleep(100);
            String string = String.format("%c-%05d", 'A' + rand.nextInt(10), ++count);
            return socket.send(string);
        }
    }

    //  .split listener thread
    //  The listener receives all messages flowing through the proxy, on its
    //  pipe. In CZMQ, the pipe is a pair of ZMQ_PAIR sockets that connect
    //  attached child threads. In other languages your mileage may vary:
    private static class Listener extends ZActor.SimpleActor
    {
        @Override
        public List<Socket> createSockets(ZContext ctx, Object... args)
        {
            return Collections.singletonList(ctx.createSocket(ZMQ.PULL));
        }

        @Override
        public void start(Socket pipe, List<Socket> sockets, ZPoller poller)
        {
            Socket subscriber = sockets.get(0);
            subscriber.connect("inproc://captured");
            poller.register(subscriber, ZPoller.IN);
        }

        @Override
        public boolean stage(Socket socket, Socket pipe, ZPoller poller, int events)
        {
            ZFrame frame = ZFrame.recvFrame(socket);
            assert (frame != null);
            frame.print(null);
            frame.destroy();
            return true;
        }
    }

    private static class Proxy extends ZProxy.Proxy.SimpleProxy
    {

        @Override
        public Socket create(ZContext ctx, Plug place, Object... args)
        {
            switch (place) {
            case FRONT:
                return ctx.createSocket(ZMQ.XSUB);
            case BACK:
                return ctx.createSocket(ZMQ.XPUB);
            case CAPTURE:
                return ctx.createSocket(ZMQ.PUSH);
            default:
                return null;
            }
        }

        @Override
        public boolean configure(Socket socket, Plug place, Object... args) throws IOException
        {
            switch (place) {
            case FRONT:
                return socket.connect("tcp://localhost:6000");
            case BACK:
                return socket.bind("tcp://*:6001");
            case CAPTURE:
                return socket.bind("inproc://captured");
            default:
                return true;
            }
        }
    }

    //  .split main thread
    //  The main task starts the subscriber and publisher, and then sets
    //  itself up as a listening proxy. The listener runs as a child thread:
    public static void main(String[] argv)
    {
        try (
             ZContext ctx = new ZContext()) {
            ZActor publisher = new ZActor(ctx, new Publisher(), "motdelafin");
            ZActor subscriber = new ZActor(ctx, new Subscriber(), "motdelafin");
            ZActor listener = new ZActor(ctx, new Listener(), "motdelafin");

            ZProxy proxy = ZProxy.newProxy(ctx, "Proxy", new Proxy(), "motdelafin");
            proxy.start(true);

            ZMQ.sleep(10);

            publisher.send("anything-sent-will-end-the-actor");
            // subscriber is already stopped after 5 receptions
            listener.send("Did I really say ANYTHING?");
            proxy.exit();

            publisher.exit().awaitSilent();
            subscriber.exit().awaitSilent();
            listener.exit().awaitSilent();
            System.out.println("Espresso Finished");
        }
    }
}
