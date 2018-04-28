package org.zeromq.proxy;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import zmq.Helper;

/**
 * In this test, we create sockets in the proxy thread, and attend that someone closes them for us.
 *
 */
public class ProxyZeroCloseTest
{
    private static class ProxyPubSub implements Runnable
    {
        private final ZContext   ctx;
        private final ZMQ.Socket xsubSocket;
        private final ZMQ.Socket xpubSocket;

        private final CountDownLatch latch = new CountDownLatch(1);

        public ProxyPubSub(ZContext ctx)
        {
            xsubSocket = ctx.createSocket(ZMQ.XSUB);
            xpubSocket = ctx.createSocket(ZMQ.XPUB);
            this.ctx = ctx;
        }

        @Override
        public void run()
        {
            try {
                xsubSocket.bindToRandomPort("tcp://127.0.0.1");
                xpubSocket.bindToRandomPort("tcp://127.0.0.1");

                latch.countDown();
                ZMQ.proxy(xsubSocket, xpubSocket, null);

            }
            catch (Throwable e) {
                latch.countDown();
            }
            finally {
                System.out.printf("<Proxy/>");
            }
        }

        public void stopProxyThread()
        {
            try {
                System.out.printf("<Sockets>");
                xpubSocket.setLinger(0);
                xpubSocket.close();

                xsubSocket.setLinger(-1);
                xsubSocket.close();
                System.out.printf("</Sockets>");
            }
            finally {
                System.out.printf("<Context>");
                ctx.close();
                System.out.printf("</Context>");
            }
        }
    }

    @Test
    public void testCloseSocketsAsynchronously() throws Exception
    {
        testProxyClose(600, 10, true);
    }

    private void testProxyClose(int loops, long sleep, boolean commandLine) throws InterruptedException, IOException
    {
        for (int index = 0; index < loops; ++index) {
            if (commandLine) {
                System.out.printf(Helper.rewind(129), index);
            }
            System.out.printf("Starting Test #%-4d ", index);
            ProxyPubSub p = new ProxyPubSub(new ZContext());
            ExecutorService executor = Executors.newFixedThreadPool(1);
            executor.submit(p);
            p.latch.await();
            ZMQ.msleep(sleep);
            p.stopProxyThread();
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);

            System.out.printf(".Finished.", index);
            if (!commandLine) {
                System.out.println();
            }
        }
        System.out.println();
    }

    @Test
    public void testCloseContext() throws Exception
    {
        testCloseContext(600, 10, true);
    }

    private void testCloseContext(int loops, long sleep, boolean commandLine) throws InterruptedException, IOException
    {
        for (int index = 0; index < loops; ++index) {
            if (commandLine) {
                System.out.printf(Helper.rewind(131), index);
            }
            System.out.printf("Closing context Test #%-4d ", index);
            ZContext ctx = new ZContext();
            ProxyPubSub p = new ProxyPubSub(ctx);
            ExecutorService executor = Executors.newFixedThreadPool(1);
            executor.submit(p);
            p.latch.await();
            ZMQ.msleep(sleep);

            System.out.printf("<Context>");
            ctx.close();
            System.out.printf("</Context>");

            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);

            System.out.printf(".Finished.", index);
            if (!commandLine) {
                System.out.println();
            }
        }
        System.out.println();
    }
}
