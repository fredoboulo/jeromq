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
import org.junit.experimental.categories.Category;

import perf.PerformanceTests;

@Category(PerformanceTests.class)
public class TestShutdownStress
{
    private static final int THREAD_COUNT = 100;

    class Worker implements Runnable
    {
        private final SocketBase socket;

        Worker(SocketBase socket) throws IOException
        {
            this.socket = socket;
        }

        @Override
        public void run()
        {
            boolean rc = ZMQ.connect(socket, "tcp://127.0.0.1:*");
            assertThat(rc, is(true));

            //  Start closing the socket while the connecting process is underway.
            ZMQ.close(socket);
        }
    }

    @Test(timeout = 4000)
    public void testShutdownStress() throws Exception
    {
        for (int idx = 0; idx < 10; idx++) {
            Ctx ctx = ZMQ.init(7);
            assertThat(ctx, notNullValue());

            SocketBase pub = ZMQ.socket(ctx, ZMQ.ZMQ_PUB);
            assertThat(pub, notNullValue());

            boolean rc = ZMQ.bind(pub, "tcp://127.0.0.1:*");
            assertThat(rc, is(true));

            ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

            for (int i = 0; i != THREAD_COUNT; i++) {
                SocketBase sub = ZMQ.socket(ctx, ZMQ.ZMQ_SUB);
                assert (sub != null);
                executor.submit(new Worker(sub));
            }

            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);

            ZMQ.close(pub);
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
