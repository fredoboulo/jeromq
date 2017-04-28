package org.zeromq.proxy;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.junit.Ignore;
import org.zeromq.Utils;
import org.zeromq.ZMQ;

import zmq.Helper;

/**
 * In this test, we create sockets in the proxy thread, and attend that someone closes them for us.
 *
 */
public class ProxyZeroCloseTest extends Thread
{
    private ZMQ.Context ctx;
    private ZMQ.Socket  xsubSocket;
    private ZMQ.Socket  xpubSocket;

    private final CountDownLatch latch = new CountDownLatch(1);

    private final int subPort;
    private final int pubPort;

    public ProxyZeroCloseTest(int subPort, int pubPort)
    {
        this.subPort = subPort;
        this.pubPort = pubPort;
    }

    @Override
    public void run()
    {
        ctx = ZMQ.context(1);
        xsubSocket = ctx.socket(ZMQ.XSUB);
        xsubSocket.bind("tcp://127.0.0.1:" + subPort);

        xpubSocket = ctx.socket(ZMQ.XPUB);
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
            //
        }
        if (ctx != null) {
            System.out.printf("Context<");
            ctx.close();
            System.out.printf("/>.....");
            ctx = null;
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException
    {
        System.out.println("ZMQ v" + ZMQ.getVersionString());
        testProxyClose(200, 50);
    }

    @Ignore
    public void testLoop200() throws Exception
    {
        testProxyClose(100, 10);
    }

    private static void testProxyClose(int loops, long sleep) throws InterruptedException, IOException
    {
        for (int index = 0; index < loops; ++index) {
            System.out.printf(Helper.rewind(129), index);
            System.out.printf("Starting Test #%-4d (", index);
            ProxyZeroCloseTest p = new ProxyZeroCloseTest(Utils.findOpenPort(), Utils.findOpenPort());
            p.start();
            p.latch.await();
            Thread.sleep(sleep);
            p.stopProxyThread();
            p.join();

            System.out.printf(").Finished.", index);
        }
        System.out.println();
    }
}
