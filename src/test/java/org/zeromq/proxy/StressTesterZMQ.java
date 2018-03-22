package org.zeromq.proxy;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.zeromq.Utils;

import zmq.Helper;

/**
 * In this test, we create sockets in the main thread, start a proxy with it, some workers,
 * and close all sockets in the main thread.
 */
public class StressTesterZMQ
{
    protected static final String PROXY_WORKERS  = "inproc://process-msg";
    protected static final String PROXY_CONTROL  = "inproc://control";
    protected static final String WORKER_PATTERN = " %s ";
    protected static final String PROXY          = "Proxy";
    protected static final String PROXY_THREAD   = PROXY;
    protected static final String EXIT           = "EXIT";
    protected static final String CLOSED         = "... Closed.";
    protected static final String CLOSING        = "Closing ...";

    protected final String frontendAddress;

    protected final int  frontEndType;
    protected final int  brokerType;
    protected final int  workerType;
    private final String label;

    protected StressTesterZMQ(String label, int frontEndType, int brokerType, int workerType) throws IOException
    {
        super();
        this.label = label;
        this.frontEndType = frontEndType;
        this.brokerType = brokerType;
        this.workerType = workerType;
        frontendAddress = "tcp://*:" + Utils.findOpenPort();
    }

    public StressTesterZMQ(int frontEndType, int brokerType, int workerType) throws IOException
    {
        this("   ZMQ", frontEndType, brokerType, workerType);
    }

    public final void testStress(int workers, int sleep, int loops) throws Exception
    {
        String print = null;
        for (int i = 0; i < loops; i++) {
            if (print != null) {
                System.out.printf(Helper.rewind(
                                                print.length() + 4 * workers + StressTesterZMQ.PROXY.length()
                                                        + StressTesterZMQ.CLOSING.length()
                                                        + StressTesterZMQ.CLOSED.length()));
            }
            print = String.format(
                                  "Test %S %s > %s > %s #%04d starting ",
                                  label,
                                  Helper.toString(frontEndType),
                                  Helper.toString(brokerType),
                                  Helper.toString(workerType),
                                  i + 1);
            System.out.printf(print);

            long start = System.currentTimeMillis();
            performTest(workers, sleep);
            long end = System.currentTimeMillis();

            String footer = String.format(" performed in %3d millis.", (end - start));
            System.out.printf(footer);
            print += footer;
        }
        System.out.println();
    }

    protected void performTest(final int workers, long sleep)
            throws InterruptedException, BrokenBarrierException, TimeoutException
    {
        final zmq.Ctx ctx = zmq.ZMQ.createContext();
        final CountDownLatch stopped = new CountDownLatch(workers + 1);
        final CyclicBarrier started = new CyclicBarrier(workers + 1);

        final zmq.SocketBase control = zmq.ZMQ.socket(ctx, zmq.ZMQ.ZMQ_PAIR);
        control.connect(PROXY_CONTROL);

        ExecutorService service = Executors.newCachedThreadPool();
        Runnable runnable = new Runnable()
        {
            @Override
            public void run()
            {
                final zmq.SocketBase recvMsgSock = zmq.ZMQ.socket(ctx, frontEndType);
                final zmq.SocketBase processMsgSock = zmq.ZMQ.socket(ctx, brokerType);
                final zmq.SocketBase controlProxy = zmq.ZMQ.socket(ctx, zmq.ZMQ.ZMQ_PAIR);

                Thread.currentThread().setName(PROXY_THREAD);
                recvMsgSock.bind(frontendAddress);
                processMsgSock.bind(PROXY_WORKERS);
                controlProxy.bind(PROXY_CONTROL);
                try {
                    zmq.ZMQ.proxy(recvMsgSock, processMsgSock, null, controlProxy);
                }
                finally {
                    for (int idx = 0; idx < workers * 10; ++idx) {
                        zmq.ZMQ.send(processMsgSock, EXIT, 0);
                    }
                    System.out.print(PROXY);
                    stopped.countDown();
                }
                recvMsgSock.close();
                controlProxy.close();
                processMsgSock.close();
            }
        };
        service.submit(runnable);

        final AtomicInteger idx = new AtomicInteger();
        for (int i = 0; i < workers; i++) {
            Runnable workerThr = new Runnable()
            {
                @Override
                public void run()
                {
                    Thread.currentThread().setName(String.format("W%d", idx.incrementAndGet()));
                    zmq.SocketBase workerSock = zmq.ZMQ.socket(ctx, workerType);
                    workerSock.connect(PROXY_WORKERS);
                    try {
                        while (true) {
                            started.await();
                            zmq.Msg msg = workerSock.recv(0);

                            if (msg == null) {
                                break;
                            }
                            if (EXIT.equals(new String(msg.data(), zmq.ZMQ.CHARSET))) {
                                break;
                            }
                            System.out.println(msg);
                            // Process the msg!
                            zmq.ZMQ.msleep(10);
                        }
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    catch (BrokenBarrierException e) {
                        e.printStackTrace();
                    }
                    finally {
                        System.out.printf(WORKER_PATTERN, Thread.currentThread().getName());
                        stopped.countDown();
                    }
                    workerSock.close();
                }
            };
            service.submit(workerThr);
        }

        started.await(1000, TimeUnit.MILLISECONDS);
        zmq.ZMQ.msleep(sleep);

        System.out.print(CLOSING);

        zmq.ZMQ.send(control, zmq.ZMQ.PROXY_TERMINATE, 0);
        zmq.ZMQ.close(control);

        stopped.await();
        zmq.ZMQ.term(ctx);
        System.out.print(CLOSED);
    }
}
