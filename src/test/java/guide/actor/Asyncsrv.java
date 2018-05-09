package guide.actor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.zeromq.ZActor;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;
import org.zeromq.ZPoller;
import org.zeromq.ZProxy;
import org.zeromq.ZProxy.Plug;

//
//Asynchronous client-to-server (DEALER to ROUTER)
//
//While this example runs in a single process, that is just to make
//it easier to start and stop the example. Each task has its own
//context and conceptually acts as a separate process.

public class Asyncsrv
{
    //---------------------------------------------------------------------
    //This is our client task
    //It connects to the server, and then sends a request once per second
    //It collects responses as they arrive, and it prints them out. We will
    //run several client tasks in parallel, each with a different random ID.

    private static final Random        rand    = new Random(System.nanoTime());
    private static final AtomicInteger counter = new AtomicInteger();

    private static class Client extends ZActor.SimpleActor
    {
        private int          requestNbr = 0;
        private final String identity   = String.format("%04X-%04X", counter.incrementAndGet(), rand.nextInt());

        @Override
        public List<Socket> createSockets(ZContext ctx, Object... args)
        {
            return Collections.singletonList(ctx.createSocket(ZMQ.DEALER));
        }

        @Override
        public void start(Socket pipe, List<Socket> sockets, ZPoller poller)
        {
            Socket client = sockets.get(0);
            client.setIdentity(identity.getBytes(ZMQ.CHARSET));
            client.connect("tcp://localhost:5570");
            poller.register(client, ZPoller.POLLIN | ZPoller.OUT);
        }

        @Override
        public boolean stage(Socket socket, Socket pipe, ZPoller poller, int events)
        {
            if ((events & ZPoller.IN) == ZPoller.IN) {
                ZMsg msg = ZMsg.recvMsg(socket);
                ZFrame content = msg.pop();
                System.out.println(identity + " " + content);
            }
            if ((events & ZPoller.OUT) == ZPoller.OUT) {
                ZMQ.msleep(100);
                socket.send(String.format("request #%02d - %s", ++requestNbr, identity));
            }

            return true;
        }
    }

    //This is our server task.
    //It uses the multithreaded server model to deal requests out to a pool
    //of workers and route replies back to clients. One worker can handle
    //one request at a time but one client can talk to multiple workers at
    //once.
    private static class Proxy extends ZProxy.Proxy.SimpleProxy
    {
        @Override
        public Socket create(ZContext ctx, Plug place, Object... args)
        {
            switch (place) {
            case FRONT:
                return ctx.createSocket(ZMQ.ROUTER);
            case BACK:
                return ctx.createSocket(ZMQ.DEALER);
            default:
                return null;
            }
        }

        @Override
        public boolean configure(Socket socket, Plug place, Object... args) throws IOException
        {
            switch (place) {
            case FRONT:
                return socket.bind("tcp://*:5570");
            case BACK:
                return socket.bind("inproc://backend");
            default:
                return true;
            }
        }
    }

    //Each worker task works on one request at a time and sends a random number
    //of replies back, with random delays between replies:

    private static class Worker extends ZActor.SimpleActor
    {
        private int       counter = 0;
        private final int id;

        public Worker(int id)
        {
            this.id = id;
        }

        @Override
        public List<Socket> createSockets(ZContext ctx, Object... args)
        {
            return Collections.singletonList(ctx.createSocket(ZMQ.DEALER));
        }

        @Override
        public void start(Socket pipe, List<Socket> sockets, ZPoller poller)
        {
            Socket worker = sockets.get(0);
            worker.setLinger(0);
            worker.setReceiveTimeOut(100);
            worker.setSendTimeOut(100);
            worker.connect("inproc://backend");
            poller.register(worker, ZPoller.IN);
        }

        @Override
        public boolean stage(Socket worker, Socket pipe, ZPoller poller, int events)
        {
            ZMsg msg = ZMsg.recvMsg(worker);
            if (msg == null) {
                return false;
            }
            ZFrame address = msg.pop();
            String request = msg.popString();
            //  Send 0..4 replies back
            final int replies = rand.nextInt(5);
            for (int reply = 0; reply < replies; reply++) {
                //  Sleep for some fraction of a second
                ZMQ.msleep(rand.nextInt(1000) + 1);
                msg = new ZMsg();
                msg.add(address);
                msg.add(String.format("worker #%s reply #%02d : %s", id, ++counter, request));
                boolean rc = msg.send(worker);
                if (!rc) {
                    return false;
                }
            }
            return true;
        }
    }

    //The main thread simply starts several clients, and a server, and then
    //waits for the server to finish.

    public static void main(String[] args) throws Exception
    {
        try (
             ZContext ctx = new ZContext()) {
            List<ZActor> actors = new ArrayList<>();

            actors.add(new ZActor(new Client(), null));
            actors.add(new ZActor(new Client(), null));
            actors.add(new ZActor(new Client(), null));

            ZProxy proxy = ZProxy.newProxy(ctx, "proxy", new Proxy(), null);
            proxy.start(true);

            //  Launch pool of worker threads, precise number is not critical
            for (int threadNbr = 0; threadNbr < 5; threadNbr++) {
                actors.add(new ZActor(ctx, new Worker(threadNbr), null));
            }

            ZMQ.sleep(5);

            proxy.pause(true);

            for (ZActor actor : actors) {
                actor.send("fini");
                actor.exit().awaitSilent();
            }
            proxy.exit();
        }
    }
}
