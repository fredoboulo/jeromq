package org.zeromq.proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

/**
 * In this test, sockets are created in the main thread and closed in each application thread.
 * Finally the context is closed in the main thread.
 *
 */
@Ignore
public class ProxyZeroTcpTest
{
    static class Client extends Thread
    {
        private Socket s    = null;
        private String name = null;

        public Client(Context ctx, String name)
        {
            s = ctx.socket(ZMQ.REQ);
            this.name = name;

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

        public Dealer(Context ctx, String name, CyclicBarrier start, CountDownLatch stopLatch)
        {
            this.start = start;
            this.stopLatch = stopLatch;
            s = ctx.socket(ZMQ.DEALER);
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

    static class Main extends Thread
    {
        Context                      ctx;
        private int                  index;
        private final AtomicBoolean  closed = new AtomicBoolean();
        private final CountDownLatch stopLatch;
        private final CyclicBarrier  start;

        Main(Context ctx, int index, CyclicBarrier start, CountDownLatch stopLatch)
        {
            this.ctx = ctx;
            this.index = index;
            this.start = start;
            this.stopLatch = stopLatch;
        }

        @Override
        public void run()
        {
            Thread.currentThread().setName("Proxy[" + index + "]");

            boolean port;
            Socket frontend = ctx.socket(ZMQ.ROUTER);

            assertNotNull(frontend);
            port = frontend.bind("tcp://127.0.0.1:6660");
            assertEquals(port, true);

            Socket backend = ctx.socket(ZMQ.DEALER);
            assertNotNull(backend);
            port = backend.bind("tcp://127.0.0.1:6661");
            assertEquals(port, true);

            try {
                start.await();
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            ZMQ.proxy(frontend, backend, null);
            closed.set(true);
            System.out.printf("<Proxy>\n");

            frontend.close();
            backend.close();

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
            testUnit(index);
            System.out.printf("----------------------------------------- .\n");
        }
    }

    private void testUnit(int index) throws Exception, InterruptedException
    {
        Context ctx = ZMQ.context(1);
        final CountDownLatch stopLatch = new CountDownLatch(3);

        final CyclicBarrier start = new CyclicBarrier(3 + 1);
        Main mt = testProxy(ctx, index, start, stopLatch);
        Assert.assertFalse("Proxy is closed before expected!", mt.closed.get());
        Thread.sleep(500);

        Assert.assertFalse("Proxy is closed before expected!", mt.closed.get());
        ctx.term();

        stopLatch.await();
    }

    public Main testProxy(Context ctx, int index, CyclicBarrier start, CountDownLatch stopLatch) throws Exception
    {
        assert (ctx != null);

        Main mt = new Main(ctx, index, start, stopLatch);
        mt.start();
        new Dealer(ctx, "AA-" + index, start, stopLatch).start();
        new Dealer(ctx, "BB-" + index, start, stopLatch).start();

        start.await();

        Thread.sleep(300); // time for the services to install

        Thread c1 = new Client(ctx, "X-" + index);
        c1.start();

        Thread c2 = new Client(ctx, "Y-" + index);
        c2.start();

        c1.join();
        c2.join();

        return mt;
    }
}
