package org.zeromq.proxy;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.zeromq.Utils;
import org.zeromq.ZMQ;

import zmq.Helper;

/**
 * In this test, we create sockets in the proxy thread, and attend that someone closes them for us.
 *
 */
public class ProxyZeroCloseTest
{
    private static class Proxy implements Runnable
    {
        private final ZMQ.Context ctx;
        private final ZMQ.Socket  xsubSocket;
        private final ZMQ.Socket  xpubSocket;

        private final CountDownLatch latch = new CountDownLatch(1);

        private final int subPort;
        private final int pubPort;

        public Proxy(ZMQ.Context ctx, int subPort, int pubPort)
        {
            xsubSocket = ctx.socket(ZMQ.XSUB);
            xpubSocket = ctx.socket(ZMQ.XPUB);
            this.subPort = subPort;
            this.pubPort = pubPort;
            this.ctx = ctx;
        }

        @Override
        public void run()
        {
            xsubSocket.bind("tcp://127.0.0.1:" + subPort);
            xpubSocket.bind("tcp://127.0.0.1:" + pubPort);

            latch.countDown();
            ZMQ.proxy(xsubSocket, xpubSocket, null);

            System.out.printf("|Proxy|");
        }

        public void stopProxyThread()
        {
            try {
                if ((xpubSocket != null) && (xsubSocket != null)) {
                    System.out.printf("Sockets<");
                    xpubSocket.setLinger(0);
                    xpubSocket.unbind("tcp://127.0.0.1:" + pubPort);
                    xpubSocket.close();

                    xsubSocket.setLinger(0);
                    xsubSocket.unbind("tcp://127.0.0.1:" + subPort);
                    xsubSocket.close();
                    System.out.printf("/>.......");
                }
            }
            finally {
                System.out.printf("Context<");
                ctx.close();
                System.out.printf("/>.....");
            }
        }
    }

    @Test
    public void testLoop600() throws Exception
    {
        testProxyClose(600, 10);
    }

    private void testProxyClose(int loops, long sleep) throws InterruptedException, IOException
    {
        for (int index = 0; index < loops; ++index) {
            System.out.printf(Helper.rewind(129), index);
            System.out.printf("Starting Test #%-4d (", index);
            Proxy p = new Proxy(ZMQ.context(1), Utils.findOpenPort(), Utils.findOpenPort());
            ExecutorService executor = Executors.newFixedThreadPool(1);
            executor.submit(p);
            p.latch.await();
            ZMQ.msleep(sleep);
            p.stopProxyThread();
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);

            System.out.printf(").Finished.", index);
        }
        System.out.println();
    }
}
