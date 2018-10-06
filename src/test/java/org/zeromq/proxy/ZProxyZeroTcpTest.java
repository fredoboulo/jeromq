package org.zeromq.proxy;

import static org.junit.Assert.assertNotNull;

import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.zeromq.SocketType;
import org.zeromq.Utils;
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

import zmq.proxy.ProxyTests;

/**
 * In this test, sockets are created in the main thread and closed in each application thread.
 * Finally the context is closed in the main thread.
 */
@Category(ProxyTests.class)
public class ZProxyZeroTcpTest
{
    private final class ProxyExtension extends Proxy.SimpleProxy
    {
        private final String clientHost;
        private final String serverHost;
        private final String captureHost;

        public ProxyExtension(String clientHost, String serverHost, String captureHost)
        {
            this.clientHost = clientHost;
            this.serverHost = serverHost;
            this.captureHost = captureHost;
        }

        @Override
        public Socket create(ZContext ctx, Plug place, Object... args)
        {
            Socket socket = null;
            switch (place) {
            case FRONT:
                Socket frontend = ctx.createSocket(SocketType.ROUTER);

                assertNotNull(frontend);
                socket = frontend;
                break;
            case BACK:
                Socket backend = ctx.createSocket(SocketType.DEALER);
                assertNotNull(backend);
                socket = backend;
                break;
            default:
                break;
            }
            return socket;
        }

        @Override
        public boolean configure(Socket socket, Plug place, Object... args)
        {
            boolean rc = true;
            if (place == Plug.FRONT) {
                rc = socket.bind(clientHost);
            }
            if (place == Plug.BACK) {
                rc = socket.bind(serverHost);
            }
            if (place == Plug.CAPTURE && socket != null) {
                socket.connect(captureHost);
            }
            return rc;
        }

        @Override
        public boolean restart(ZMsg cfg, Socket socket, Plug place, Object... args)
        {
            boolean rc = true;
            switch (place) {
            case FRONT:
                rc = socket.unbind(clientHost);
                Assert.assertTrue("unable to unbind frontend", rc);

                rc = socket.bind(clientHost);
                Assert.assertTrue("unable to bind frontend", rc);
                break;
            case BACK:
                rc = socket.unbind(serverHost);
                Assert.assertTrue("unable to unbind frontend", rc);

                rc = socket.bind(serverHost);
                Assert.assertTrue(rc);
                break;
            default:
                break;
            }
            return false;
        }

        @Override
        public boolean configure(Socket pipe, ZMsg cfg, Socket frontend, Socket backend, Socket capture, Object... args)
        {
            return true;
        }
    }

    static class Client implements Runnable
    {
        private final Socket socket;
        private final String name;
        private final String clientHost;

        public Client(ZContext ctx, String clientHost, String name)
        {
            this.clientHost = clientHost;
            this.name = name;

            socket = ctx.createSocket(SocketType.REQ);
            socket.setIdentity(name.getBytes(ZMQ.CHARSET));
        }

        @Override
        public void run()
        {
            Thread.currentThread().setName("Client[" + name + "]");
            System.out.printf("<Client %s>", name);
            socket.connect(clientHost);
            socket.send("hello", 0);
            socket.recvStr(0);
            socket.send("world", 0);
            socket.recvStr(0);

            socket.close();
            System.out.printf("</Client %s>", name);
        }
    }

    static class Worker implements Runnable
    {
        private final Socket         socket;
        private final String         name;
        private final CountDownLatch stopLatch;
        private final CyclicBarrier  start;
        private final String         serverHost;

        public Worker(ZContext ctx, String serverHost, String name, CyclicBarrier start, CountDownLatch stopLatch)
        {
            this.serverHost = serverHost;
            this.start = start;
            this.stopLatch = stopLatch;
            socket = ctx.createSocket(SocketType.DEALER);
            this.name = name;

            socket.setIdentity(name.getBytes(ZMQ.CHARSET));
        }

        @Override
        public void run()
        {
            Thread.currentThread().setName("Dealer[" + name + "]");
            System.out.printf("<Worker %s>", name);

            socket.connect(serverHost);

            try {
                start.await();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            int count = 0;
            while (count < 2) {
                String msg = socket.recvStr(0);
                if (msg == null) {
                    throw new RuntimeException();
                }
                String identity = msg;
                msg = socket.recvStr(0);
                if (msg == null) {
                    throw new RuntimeException();
                }

                msg = socket.recvStr(0);
                if (msg == null) {
                    throw new RuntimeException();
                }
                String data = msg;

                socket.send(identity, ZMQ.SNDMORE);
                socket.send((byte[]) null, ZMQ.SNDMORE);

                String response = "OK " + data;

                socket.send(response, 0);
                count++;
            }
            socket.close();
            System.out.printf("</Worker %s>", name);
            stopLatch.countDown();
        }
    }

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
            System.out.println("-----------------------------------------");
            testUnit(index);
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
        System.out.println("!");
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
                System.out.printf("<Proxy>");
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
                System.out.printf("</Proxy>");
                stopLatch.countDown();
                return super.destroyed(ctx, pipe, poller);
            }
        };
        ExecutorService executor = Executors.newCachedThreadPool();

        final String clientHost = "tcp://127.0.0.1:" + Utils.findOpenPort();
        final String serverHost = "tcp://127.0.0.1:" + Utils.findOpenPort();
        final String captureHost = "tcp://127.0.0.1:" + Utils.findOpenPort();

        final Proxy proxyImpl = new ProxyExtension(clientHost, serverHost, captureHost);

        ZProxy proxy = ZProxy.newZProxy(ctx, "Proxy[" + index + "]", proxyImpl, null, dubber);
        executor.submit(new Worker(ctx, serverHost, "AA-" + index, start, stopLatch));
        executor.submit(new Worker(ctx, serverHost, "BB-" + index, start, stopLatch));

        start.await();
        String status = proxy.start(false);
        Assert.assertNotEquals("Proxy is not started", ZProxy.STARTED, status);
        Thread.sleep(300); // time for the services to install
        status = proxy.status();
        Assert.assertEquals("Proxy is not started", ZProxy.STARTED, status);

        executor.submit(new Client(ctx, clientHost, "X-" + index));

        executor.submit(new Client(ctx, clientHost, "Y-" + index));
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        return proxy;
    }
}
