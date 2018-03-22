package org.zeromq.proxy;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import zmq.Helper;

/**
 * In this test, we create sockets in the proxy thread, and attend that someone closes them for us.
 *
 */
public class ProxyCloseZeroTest
{
    private static final class ProxyClose extends Thread
    {
        private ZContext ctx;
        private Socket   front;
        private Socket   back;

        private final CountDownLatch latch = new CountDownLatch(1);

        public ProxyClose()
        {
            super();
        }

        @Override
        public void run()
        {
            if (ctx == null) {
                ctx = new ZContext();
            }
            front = ctx.createSocket(ZMQ.XSUB);
            assertThat(front, notNullValue());
            front.bind("tcp://127.0.0.1:7000");

            back = ctx.createSocket(ZMQ.XPUB);
            assertThat(back, notNullValue());
            back.bind("tcp://127.0.0.1:7001");

            latch.countDown();
            boolean rc = ZMQ.proxy(front, back, null);
            //            assertThat(rc, is(false));

            close(front, back);

            System.out.printf("|Proxy %s|", rc);
        }

        private void close(Socket front, Socket back)
        {
            System.out.print("Sockets<");

            //            ctx.destroySocket(back);
            //            ctx.destroySocket(front);
            back.close();
            front.close();

            System.out.print("/>.....");
        }

        void await()
        {
            try {
                latch.await();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        public void stopProxyThread()
        {
            if (ctx != null) {
                System.out.print("Context<");
                ctx.setLinger(0);
                ctx.close();
                System.out.print("/>.....");
                ctx = null;
            }
        }
    }

    private static String getVersionString()
    {
        return zmq.ZMQ.ZMQ_VERSION_MAJOR + "." + zmq.ZMQ.ZMQ_VERSION_MINOR + "." + zmq.ZMQ.ZMQ_VERSION_PATCH;
    }

    @Test
    public void testProxyCloseLinger0() throws InterruptedException
    {
        System.out.println("Close Proxy with linger 0");
        for (int idx = 250; idx > 0; idx--) {
            System.out.print(Helper.erase(110));
            test(idx, new ProxyClose());
            //            System.out.println();
        }
        System.out.println();
    }

    private void test(int idx, ProxyClose proxy) throws InterruptedException
    {
        System.out.printf("%s ZMQ v %s ", getClass().getSimpleName(), getVersionString());
        long start = System.currentTimeMillis();
        proxy.start();
        proxy.await();
        ZMQ.msleep(15);
        proxy.stopProxyThread();
        proxy.join();
        long end = System.currentTimeMillis();

        System.out.printf(". Test #%d finished in %4d millis.", idx, (end - start));
        ZMQ.msleep(30);
    }
}
