package org.zeromq;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;
import org.zeromq.ZMQ.Context;

public class DealerDealerTest
{
    private final class Client implements Runnable
    {
        private final Context       context;
        private final boolean       verbose;
        private final String        host;
        private final int           messagesCount;
        private final int           reconnectAfter;
        private final Deque<String> queue;

        private int missed     = 0;
        private int reconnects = 0;
        private int count      = 0;
        private int received   = 0;

        private final List<ZMQ.Socket> sockets = new ArrayList<>();

        private Client(Context context, boolean verbose, String host, int messagesCount, int reconnectAfter,
                Deque<String> queue)
        {
            this.context = context;
            this.verbose = verbose;
            this.host = host;
            this.messagesCount = messagesCount;
            this.reconnectAfter = reconnectAfter;
            this.queue = queue;
        }

        @Override
        public void run()
        {
            ZMQ.Socket worker = context.socket(SocketType.DEALER);
            worker.connect(host);

            int msg = messagesCount;
            while (msg-- > 0) {
                String received = worker.recvStr();
                String expected = queue.pop();
                count++;

                while (!expected.equals(received)) {
                    count++;
                    missed++;
                    System.out.println("Missed " + expected + " (received " + received + ")");
                    expected = queue.pop();
                }

                this.received++;
                if (verbose) {
                    System.out.println("Received " + received);
                }

                if (count % reconnectAfter == 0) {
                    worker.disconnect(host);
                    reconnects++;
                    if (verbose) {
                        System.out.println("Reconnecting...");
                    }
                    sockets.add(worker);
                    worker = context.socket(SocketType.DEALER);
                    worker.connect(host);
                }

                if (count % (messagesCount / 10) == 0) {
                    System.out.println(
                                       "Received: " + this.received + " missed: " + missed + " reconnects: "
                                               + reconnects);
                    for (ZMQ.Socket socket : sockets) {
                        socket.close();
                    }
                    sockets.clear();
                }
            }
            worker.close();
            for (ZMQ.Socket socket : sockets) {
                socket.close();
            }
            sockets.clear();
        }
    }

    @Test
    @Ignore
    public void testRepeated() throws InterruptedException, IOException
    {
        for (int idx = 0; idx < 100; idx++) {
            System.out.println("+++++++++++ " + idx);
            testIssue335();
        }
    }

    @Test
    public void testIssue335() throws InterruptedException, IOException
    {
        final boolean verbose = false;
        final int messagesCount = 200;
        final int reconnectAfter = 2;
        final int sleep = 10;

        final ZMQ.Context context = ZMQ.context(1);
        final Deque<String> queue = new LinkedBlockingDeque<>();
        final String host = "tcp://localhost:" + Utils.findOpenPort();

        final Runnable server = new Runnable()
        {
            @Override
            public void run()
            {
                final ZMQ.Socket server = context.socket(SocketType.DEALER);
                server.bind(host);
                int msg = messagesCount;

                while (msg-- > 0) {
                    final String payload = Integer.toString(msg);
                    queue.add(payload);
                    if (!server.send(payload)) {
                        System.out.println("Send failed");
                    }

                    zmq.ZMQ.msleep(sleep);
                }
                server.close();
            }
        };

        final Client client = new Client(context, verbose, host, messagesCount, reconnectAfter, queue);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.submit(server);
        executor.submit(client);

        long start = System.currentTimeMillis();
        executor.shutdown();
        executor.awaitTermination(messagesCount * sleep * 2, TimeUnit.MILLISECONDS);
        long end = System.currentTimeMillis();
        System.out.println("Done in  " + (end - start) + " millis.");

        assertThat(client.missed, is(0));
        assertThat(client.reconnects, is(messagesCount / reconnectAfter));
        assertThat(client.count, is(client.received));

        context.close();
    }
}
