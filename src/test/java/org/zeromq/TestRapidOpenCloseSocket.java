package org.zeromq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;
import org.zeromq.ZMQ.Socket;

// For issue: https://github.com/zeromq/jeromq/issues/198
public class TestRapidOpenCloseSocket
{
    @Test
    @Ignore
    public void testRapidOpenCloseSocket() throws Exception
    {
        for (int i = 0; i < 5000; i++) {
            System.out.println("++++ " + i);
            performTest();
        }
    }

    private void performTest() throws Exception
    {
        ZContext ctx = new ZContext();

        Socket recvMsgSock = ctx.createSocket(SocketType.PULL);
        recvMsgSock.bind("tcp://*:" + Utils.findOpenPort());
        Socket processMsgSock = ctx.createSocket(SocketType.PUSH);
        processMsgSock.bind("inproc://process-msg");

        List<Socket> workerSocks = new ArrayList<Socket>();
        for (int i = 0; i < 5; i++) {
            Socket workerSock = ctx.createSocket(SocketType.PULL);
            workerSock.connect("inproc://process-msg");
            workerSocks.add(workerSock);
        }

        ExecutorService service = Executors.newFixedThreadPool(workerSocks.size() + 1);

        final ZMQQueue queue = new ZMQQueue(ctx.getContext(), recvMsgSock, processMsgSock);
        service.submit(new Runnable()
        {
            @Override
            public void run()
            {
                Thread.currentThread().setName("Proxy thr");
                queue.run();
            }
        });

        for (final Socket workerSock : workerSocks) {
            service.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    Thread.currentThread().setName("A worker thread");
                    while (true) {
                        byte[] msg = workerSock.recv();
                        // Process the msg!
                        System.out.println(Arrays.toString(msg));
                    }
                }
            });
        }

        ZMQ.msleep(100);
        ctx.close();

        service.shutdown();
        service.awaitTermination(10, TimeUnit.SECONDS);
    }
}
