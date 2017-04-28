package org.zeromq.proxy;

import static org.junit.Assert.assertNotNull;

import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.zeromq.ZActor;
import org.zeromq.ZActor.Actor;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;
import org.zeromq.ZPoller;
import org.zeromq.ZProxy;
import org.zeromq.ZProxy.Plug;
import org.zeromq.ZProxy.Proxy;

/**
 * In this test, sockets are created in the main thread and closed in each application thread.
 * Finally the context is closed in the main thread.
 */
@Ignore
public class ZProxyZeroTcpTest
{
    static class Client extends Thread
    {
        private Socket s    = null;
        private String name = null;

        public Client(ZContext ctx, String name)
        {
            this.name = name;

            s = ctx.createSocket(ZMQ.REQ);
            s.setIdentity(name.getBytes(ZMQ.CHARSET));
        }

        @Override
        public void run()
        {
            Thread.currentThread().setName("Client[" + name + "]");
            s.connect("tcp://127.0.0.1:6660");
            s.send("hello", 0);
            System.out.printf("Client %s sent '%s'.\n", name, "hello");
            String msg = s.recvStr(0);
            System.out.printf("Client %s received '%s'.\n", name, msg);
            s.send("world", 0);
            System.out.printf("Client %s sent '%s'.\n", name, "world");
            msg = s.recvStr(0);

            s.close();
            System.out.printf("<Client %s>\n", name);
        }
    }

    static class Dealer extends Thread
    {
        private Socket               s    = null;
        private String               name = null;
        private final CountDownLatch stopLatch;
        private final CyclicBarrier  start;

        public Dealer(ZContext ctx, String name, CyclicBarrier start, CountDownLatch stopLatch)
        {
            this.start = start;
            this.stopLatch = stopLatch;
            s = ctx.createSocket(ZMQ.DEALER);
            this.name = name;

            s.setIdentity(name.getBytes(ZMQ.CHARSET));
        }

        @Override
        public void run()
        {
            Thread.currentThread().setName("Dealer[" + name + "]");
            System.out.println("Start dealer " + name);

            s.connect("tcp://127.0.0.1:6661");

            try {
                start.await();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            int count = 0;
            while (count < 2) {
                String msg = s.recvStr(0);
                if (msg == null) {
                    throw new RuntimeException();
                }
                String identity = msg;
                System.out.printf("Worker %s received client identity %s", name, identity);
                msg = s.recvStr(0);
                if (msg == null) {
                    throw new RuntimeException();
                }
                System.out.printf(" %s bottom ", name, msg);

                msg = s.recvStr(0);
                if (msg == null) {
                    throw new RuntimeException();
                }
                String data = msg;

                System.out.printf(" %s data %s\n", name, msg, data);
                s.send(identity, ZMQ.SNDMORE);
                s.send((byte[]) null, ZMQ.SNDMORE);

                String response = "OK " + data;

                s.send(response, 0);
                count++;
            }
            s.close();
            System.out.printf("<Worker %s>\n", name);
            stopLatch.countDown();
        }
    }

    private final Proxy sockets = new Proxy.SimpleProxy()
    {
        @Override
        public Socket create(ZContext ctx, Plug place, Object[] args)
        {
            Socket socket = null;
            switch (place) {
            case FRONT:
                Socket frontend = ctx.createSocket(ZMQ.ROUTER);

                assertNotNull(frontend);
                socket = frontend;
                break;
            case BACK:
                Socket backend = ctx.createSocket(ZMQ.DEALER);
                assertNotNull(backend);
                socket = backend;
                break;
            default:
                break;
            }
            return socket;
        }

        @Override
        public boolean configure(Socket socket, Plug place, Object[] args)
        {
            boolean rc = true;
            if (place == Plug.FRONT) {
                rc = socket.bind("tcp://127.0.0.1:6660");
            }
            if (place == Plug.BACK) {
                rc = socket.bind("tcp://127.0.0.1:6661");
            }
            if (place == Plug.CAPTURE && socket != null) {
                socket.connect("tcp://127.0.0.1:4263");
            }
            return rc;
        }

        @Override
        public boolean restart(ZMsg cfg, Socket socket, Plug place, Object[] args)
        {
            boolean rc = true;
            switch (place) {
            case FRONT:
                rc = socket.unbind("tcp://127.0.0.1:6660");
                Assert.assertTrue("unable to unbind frontend", rc);

                rc = socket.bind("tcp://127.0.0.1:6660");
                Assert.assertTrue("unable to bind frontend", rc);
                break;
            case BACK:
                rc = socket.unbind("tcp://127.0.0.1:6661");
                Assert.assertTrue("unable to unbind frontend", rc);

                rc = socket.bind("tcp://127.0.0.1:6661");
                Assert.assertTrue(rc);
                break;
            default:
                break;
            }
            return false;
        }

        @Override
        public boolean configure(Socket pipe, ZMsg cfg, Socket frontend, Socket backend, Socket capture, Object[] args)
        {
            return true;
        }
    };

    @Test
    public void testProxy() throws Exception
    {
        int index = 0;
        testUnit(index);
    }

    @Test
    public void testProxyRepeated() throws Exception
    {
        for (int index = 0; index < 10; ++index) {
            testUnit(index);
            System.out.printf("----------------------------------------- .\n");
        }
    }

    private void testUnit(int index) throws Exception, InterruptedException
    {
        ZContext ctx = new ZContext(1);
        final CountDownLatch stopLatch = new CountDownLatch(3);

        final CyclicBarrier start = new CyclicBarrier(3 + 1);
        ZProxy proxy = testProxy(ctx, index, start, stopLatch);
        Assert.assertEquals("Proxy is closed before expected!", ZProxy.STARTED, proxy.status(false));
        Thread.sleep(500);

        Assert.assertEquals("Proxy is closed before expected!", ZProxy.STARTED, proxy.status());
        System.out.printf(".");
        String status = proxy.exit();
        Assert.assertEquals("Proxy is not closed after stop of the proxy!", ZProxy.EXITED, status);
        Assert.assertEquals("Proxy is not closed after stop of the proxy!", ZProxy.EXITED, proxy.status());
        System.out.printf(".");
        ctx.close();
        System.out.printf(".");

        stopLatch.await();
        System.out.printf("!");
        Assert.assertEquals("Proxy is not closed after close of the context!", ZProxy.EXITED, proxy.status());
    }

    public ZProxy testProxy(ZContext ctx, int index, final CyclicBarrier start, final CountDownLatch stopLatch)
            throws Exception
    {
        assert (ctx != null);

        Actor dubber = new ZActor.SimpleActor()
        {
            private final AtomicBoolean closed = new AtomicBoolean();

            @Override
            public void start(Socket pipe, List<Socket> sockets, ZPoller poller)
            {
                super.start(pipe, sockets, poller);
                try {
                    start.await();
                }
                catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                catch (BrokenBarrierException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            @Override
            public boolean destroyed(ZContext ctx, Socket pipe, ZPoller poller)
            {
                closed.set(true);
                System.out.printf("<Proxy>\n");
                stopLatch.countDown();
                return super.destroyed(ctx, pipe, poller);
            }
        };
        ZProxy proxy = ZProxy.newZProxy(ctx, "Proxy[" + index + "]", null, sockets, null, dubber);
        new Dealer(ctx, "AA-" + index, start, stopLatch).start();
        new Dealer(ctx, "BB-" + index, start, stopLatch).start();

        start.await();
        String status = proxy.start(false);
        Assert.assertNotEquals("Proxy is not started", ZProxy.STARTED, status);
        Thread.sleep(300); // time for the services to install
        status = proxy.status();
        Assert.assertEquals("Proxy is not started", ZProxy.STARTED, status);

        Thread c1 = new Client(ctx, "X-" + index);
        c1.start();

        Thread c2 = new Client(ctx, "Y-" + index);
        c2.start();

        c1.join();
        c2.join();

        return proxy;
    }
}
