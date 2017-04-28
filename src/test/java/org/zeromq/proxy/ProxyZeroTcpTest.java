package org.zeromq.proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;
import org.zeromq.Utils;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

/**
 * In this test, sockets are created in the main thread and closed in each application thread.
 * Finally the context is closed in the main thread.
 *
 */
public class ProxyZeroTcpTest
{
    static class Client implements Runnable
    {
        private final Socket socket;
        private final String name;
        private final String clientHost;

        public Client(Context ctx, String clientHost, String name)
        {
            this.clientHost = clientHost;
            socket = ctx.socket(ZMQ.REQ);
            this.name = name;

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

    static class Server implements Runnable
    {
        private final Socket         socket;
        private final String         name;
        private final CountDownLatch stopLatch;
        private final CyclicBarrier  start;
        private final String         serverHost;

        public Server(Context ctx, String serverHost, String name, CyclicBarrier start, CountDownLatch stopLatch)
        {
            this.serverHost = serverHost;
            this.start = start;
            this.stopLatch = stopLatch;
            socket = ctx.socket(ZMQ.DEALER);
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

    static class Proxy extends Thread
    {
        Context                      ctx;
        private int                  index;
        private final AtomicBoolean  closed = new AtomicBoolean();
        private final CountDownLatch stopLatch;
        private final CyclicBarrier  start;

        private final String clientHost;
        private final String serverHost;

        Proxy(Context ctx, int index, String clientHost, String serverHost, CyclicBarrier start,
                CountDownLatch stopLatch)
        {
            this.ctx = ctx;
            this.index = index;
            this.clientHost = clientHost;
            this.serverHost = serverHost;
            this.start = start;
            this.stopLatch = stopLatch;
        }

        @Override
        public void run()
        {
            Thread.currentThread().setName("Proxy[" + index + "]");

            System.out.printf("<Proxy>");
            boolean port;
            Socket frontend = ctx.socket(ZMQ.ROUTER);

            assertNotNull(frontend);
            port = frontend.bind(clientHost);
            assertEquals(port, true);

            Socket backend = ctx.socket(ZMQ.DEALER);
            assertNotNull(backend);
            port = backend.bind(serverHost);
            assertEquals(port, true);

            try {
                start.await();
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            ZMQ.proxy(frontend, backend, null);
            closed.set(true);
            System.out.printf("</Proxy>");

            frontend.close();
            backend.close();

            stopLatch.countDown();
        }

    }

    @Test
    public void testProxy() throws Exception
    {
        testUnit(0);
    }

    @Test
    public void testProxyRepeated() throws Exception
    {
        for (int index = 0; index < 10; ++index) {
            System.out.println("----------------------------------------- ");
            testUnit(index);
        }
    }

    private void testUnit(int index) throws Exception, InterruptedException
    {
        Context ctx = ZMQ.context(1);
        final CountDownLatch stopLatch = new CountDownLatch(3);

        final CyclicBarrier start = new CyclicBarrier(3 + 1);
        Proxy mt = testProxy(ctx, index, start, stopLatch);
        Assert.assertFalse("Proxy is closed before expected!", mt.closed.get());
        Thread.sleep(500);

        Assert.assertFalse("Proxy is closed before expected!", mt.closed.get());
        ctx.term();

        System.out.println();
        stopLatch.await();
    }

    public Proxy testProxy(Context ctx, int index, CyclicBarrier start, CountDownLatch stopLatch) throws Exception
    {
        assert (ctx != null);

        final String clientHost = "tcp://127.0.0.1:" + Utils.findOpenPort();
        final String serverHost = "tcp://127.0.0.1:" + Utils.findOpenPort();

        ExecutorService executor = Executors.newCachedThreadPool();

        Proxy mt = new Proxy(ctx, index, clientHost, serverHost, start, stopLatch);

        new Thread(mt).start();
        executor.submit(new Server(ctx, serverHost, "AA-" + index, start, stopLatch));
        executor.submit(new Server(ctx, serverHost, "BB-" + index, start, stopLatch));

        start.await();

        Thread.sleep(300); // time for the services to install

        executor.submit(new Client(ctx, clientHost, "X-" + index));
        executor.submit(new Client(ctx, clientHost, "Y-" + index));

        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        return mt;
    }
}
