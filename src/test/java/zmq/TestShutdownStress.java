package zmq;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

public class TestShutdownStress
{
    private static final int THREAD_COUNT = 100;

    class Worker implements Runnable
    {
        private final int        port;
        private final SocketBase socket;

        Worker(SocketBase socket) throws IOException
        {
            this.port = Utils.findOpenPort();
            this.socket = socket;
        }

        @Override
        public void run()
        {
            boolean rc = ZMQ.connect(socket, "tcp://127.0.0.1:" + port);
            assertThat(rc, is(true));

            //  Start closing the socket while the connecting process is underway.
            ZMQ.closeZeroLinger(socket);
        }
    }

    @Test
    public void testShutdownStress() throws Exception
    {
        int randomPort = Utils.findOpenPort();

        for (int idx = 0; idx < 10; idx++) {
            System.out.println("---------- " + idx);
            Ctx ctx = ZMQ.init(7);
            assertThat(ctx, notNullValue());

            SocketBase pub = ZMQ.socket(ctx, ZMQ.ZMQ_PUB);
            assertThat(pub, notNullValue());

            boolean rc = ZMQ.bind(pub, "tcp://127.0.0.1:" + randomPort);
            assertThat(rc, is(true));

            ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

            for (int i = 0; i != THREAD_COUNT; i++) {
                SocketBase sub = ZMQ.socket(ctx, ZMQ.ZMQ_SUB);
                assert (sub != null);
                executor.submit(new Worker(sub));
            }

            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);

            ZMQ.closeZeroLinger(pub);
            ZMQ.term(ctx);
        }
    }

    @Test
    @Ignore
    public void testRepeated() throws Exception
    {
        for (int idx = 0; idx < 400; idx++) {
            System.out.println("---------- " + idx + " ---------- ");
            testShutdownStress();
        }
    }
}
