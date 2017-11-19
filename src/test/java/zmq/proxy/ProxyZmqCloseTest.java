package zmq.proxy;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.junit.Ignore;
import org.junit.Test;

import zmq.Ctx;
import zmq.Helper;
import zmq.SocketBase;
import zmq.ZMQ;

/**
 * In this test, we create sockets in the proxy thread, and attend that someone closes them for us.
 * @author fred
 *
 *
 */
public class ProxyZmqCloseTest extends Thread
{
    @Ignore
    public static final class CloseUnBinder extends ProxyZmqCloseTest
    {
        public CloseUnBinder()
        {
            super();
        }

        @Override
        protected void stop(SocketBase front, SocketBase back)
        {
            boolean rc = false;

            ZMQ.setSocketOption(front, ZMQ.ZMQ_LINGER, 0);
            rc = ZMQ.unbind(front, "tcp://127.0.0.1:7001");
            assertThat(rc, is(true));

            ZMQ.close(front);

            ZMQ.setSocketOption(back, ZMQ.ZMQ_LINGER, 0);
            rc = ZMQ.unbind(back, "tcp://127.0.0.1:7000");
            assertThat(rc, is(true));
            ZMQ.close(back);
        }
    }

    @Ignore
    public static final class KOCloseLinger10 extends ProxyZmqCloseTest
    {
        public KOCloseLinger10()
        {
            super();
        }

        @Override
        protected void stop(SocketBase front, SocketBase back)
        {
            ZMQ.setSocketOption(front, ZMQ.ZMQ_LINGER, 10);
            ZMQ.close(front);
            ZMQ.setSocketOption(back, ZMQ.ZMQ_LINGER, 10);
            ZMQ.close(back);
        }
    }

    @Ignore
    public static final class CloseLinger0 extends ProxyZmqCloseTest
    {
        public CloseLinger0()
        {
            super();
        }

        @Override
        protected void stop(SocketBase front, SocketBase back)
        {
            ZMQ.setSocketOption(front, ZMQ.ZMQ_LINGER, 0);
            ZMQ.close(front);
            ZMQ.setSocketOption(back, ZMQ.ZMQ_LINGER, 0);
            ZMQ.close(back);
        }
    }

    @Ignore
    public static final class KOSimpleclose extends ProxyZmqCloseTest
    {
        public KOSimpleclose()
        {
            super();
        }

        @Override
        protected void stop(SocketBase front, SocketBase back)
        {
            ZMQ.close(front);
            ZMQ.close(back);
        }
    }

    private Ctx        ctx;
    private SocketBase frontSocket;
    private SocketBase backSocket;

    public ProxyZmqCloseTest()
    {
        this(null);
    }

    private ProxyZmqCloseTest(Ctx ctx)
    {
        this.ctx = ctx;
    }

    @Override
    public void run()
    {
        if (ctx == null) {
            ctx = ZMQ.init(1);
        }
        frontSocket = ZMQ.socket(ctx, ZMQ.ZMQ_XSUB);
        assertThat(frontSocket, notNullValue());
        boolean rc = ZMQ.bind(frontSocket, "tcp://127.0.0.1:7000");
        assertThat(rc, is(true));

        backSocket = ZMQ.socket(ctx, ZMQ.ZMQ_XPUB);
        assertThat(backSocket, notNullValue());
        rc = ZMQ.bind(backSocket, "tcp://127.0.0.1:7001");
        assertThat(rc, is(true));

        rc = ZMQ.proxy(frontSocket, backSocket, null);
        //        assertThat(rc, is(false));

        // not closing the sockets as someone does it for us
        //        ZMQ.zmq_close(frontSocket);
        //        ZMQ.zmq_close(backSocket);

        System.out.printf("|Proxy %s|", rc);
    }

    public void stopProxyThread()
    {
        try {
            SocketBase back = backSocket;
            SocketBase front = frontSocket;
            if ((back != null) && (front != null)) {
                System.out.print("Sockets<");
                stop(front, back);

                System.out.print("/>.....");
            }
        }
        catch (Throwable t) {
            t.printStackTrace();
            fail(t.getLocalizedMessage());
        }
        finally {
            //
        }
        close();
    }

    private void close()
    {
        if (ctx != null) {
            System.out.print("Context<");
            ZMQ.term(ctx);
            System.out.print("/>.....");
            ctx = null;
        }
    }

    protected void stop(SocketBase front, SocketBase back)
    {
        ZMQ.setSocketOption(back, ZMQ.ZMQ_LINGER, 0);

        boolean rc = false;

        //                rc = ZMQ.zmq_unbind(xpubSocket, "tcp://127.0.0.1:7001");
        //                assertThat(rc, is(true));
        ZMQ.close(back);

        ZMQ.setSocketOption(front, ZMQ.ZMQ_LINGER, 0);
        //                rc = ZMQ.zmq_unbind(xsubSocket, "tcp://127.0.0.1:7000");
        //                assertThat(rc, is(true));

        ZMQ.close(front);
    }

    public static void main(String[] args) throws InterruptedException
    {
        System.out.printf("%s ZMQ v%s %s ", ProxyZmqCloseTest.class.getSimpleName(), " ZMQ v", getVersionString());
        for (int idx = 5; idx > 0; idx--) {
            ProxyZmqCloseTest p = new ProxyZmqCloseTest();
            p.start();
            Thread.sleep(1000);
            p.stopProxyThread();
            p.join();

            System.out.printf("Test #%d finished.\n", idx);
        }
    }

    private static String getVersionString()
    {
        return zmq.ZMQ.ZMQ_VERSION_MAJOR + "." + zmq.ZMQ.ZMQ_VERSION_MINOR + "." + zmq.ZMQ.ZMQ_VERSION_PATCH;
    }

    //    @Test
    @Deprecated
    public void testProxyNoClose() throws InterruptedException
    {
        // no no no
        System.out.println("ProxyThreadZmq.testProxyNoClose()");
        test(-1, new ProxyZmqCloseTest());
    }

    //    @Test
    @Deprecated
    public void testProxyClose1() throws InterruptedException
    {
        // no no no
        System.out.println("ProxyThreadZmq.testProxyNoClose()");
        Ctx ctx = ZMQ.init(1);
        ProxyZmqCloseTest proxy = new ProxyZmqCloseTest(ctx);
        System.out.println(getClass().getSimpleName() + " ZMQ v" + getVersionString());
        long start = System.currentTimeMillis();
        proxy.start();
        Thread.sleep(1000);
        ZMQ.term(ctx);
        //        proxy.stopProxyThread();
        //        proxy.join();
        long end = System.currentTimeMillis();

        System.out.printf("Test #%d finished in %4d millis.\n", 421, (end - start));
    }

    //    @Test
    @Deprecated
    public void testProxyClose() throws InterruptedException
    {
        // no no no
        test(-1, new KOSimpleclose());
    }

    @Test
    public void testProxyCloseLinger0() throws InterruptedException
    {
        System.out.println("Close Proxy with linger 0");
        for (int idx = 25; idx > 0; idx--) {
            test(idx, new CloseLinger0());
        }
        System.out.println();
    }

    //    @Test
    @Deprecated
    public void testProxyCloseLinger10() throws InterruptedException
    {
        System.out.println("ProxyThreadZmq.testProxyCloseLinger10()");
        test(-1, new KOCloseLinger10());
    }

    //    @Test
    public void testProxyUnbind() throws InterruptedException
    {
        for (int idx = 5; idx > 0; idx--) {
            test(idx, new CloseUnBinder());
            System.out.printf("Test #%d finished.\n", idx);
        }
    }

    public void test(int idx, ProxyZmqCloseTest proxy) throws InterruptedException
    {
        System.out.print(Helper.rewind(130));
        System.out.printf("%s ZMQ v%s %s ", ProxyZmqCloseTest.class.getSimpleName(), " ZMQ v", getVersionString());
        long start = System.currentTimeMillis();
        proxy.start();
        Thread.sleep(500);
        proxy.stopProxyThread();
        proxy.join();
        long end = System.currentTimeMillis();

        System.out.printf(". Test #%d finished in %4d millis.", idx, (end - start));
    }
}
