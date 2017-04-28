package org.zeromq.proxy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

import org.zeromq.Utils;

/**
 * In this test, we create sockets in the main thread, start a proxy with it, some workers,
 * and close all sockets in the main thread.
 */
public class StressTesterZMQ
{
    protected static final String PROXY_WORKERS  = "inproc://process-msg";
    protected static final String WORKER_PATTERN = " %s ";
    protected static final String PROXY          = "Proxy";
    protected static final String PROXY_THREAD   = PROXY;
    protected static final String CLOSED         = "... Closed.";
    protected static final String CLOSING        = "Closing ...";

    protected final String frontendAddress;

    protected final int frontEndType;
    protected final int brokerType;
    protected final int workerType;

    public StressTesterZMQ(int frontEndType, int brokerType, int workerType) throws IOException
    {
        super();
        this.frontEndType = frontEndType;
        this.brokerType = brokerType;
        this.workerType = workerType;
        frontendAddress = "tcp://*:" + Utils.findOpenPort();
    }

    protected void performTest(int workers, long sleep)
            throws InterruptedException, BrokenBarrierException, TimeoutException
    {
        final zmq.Ctx ctx = zmq.ZMQ.createContext();
        final zmq.SocketBase recvMsgSock = zmq.ZMQ.socket(ctx, frontEndType);
        recvMsgSock.bind(frontendAddress);
        final zmq.SocketBase processMsgSock = zmq.ZMQ.socket(ctx, brokerType);
        processMsgSock.bind(PROXY_WORKERS);

        final CountDownLatch stopped = new CountDownLatch(workers + 1);
        final CyclicBarrier started = new CyclicBarrier(workers + 1);
        List<zmq.SocketBase> workerSocks = new ArrayList<zmq.SocketBase>();
        for (int i = 0; i < workers; i++) {
            zmq.SocketBase workerSock = zmq.ZMQ.socket(ctx, workerType);
            workerSock.connect(PROXY_WORKERS);
            workerSocks.add(workerSock);
        }

        Runnable runnable = new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    zmq.ZMQ.proxy(recvMsgSock, processMsgSock, null);
                }
                finally {
                    System.out.print(PROXY);
                    stopped.countDown();
                }
            }
        };
        Thread proxyThr = new Thread(runnable, PROXY_THREAD);
        proxyThr.start();

        int idx = 0;
        for (final zmq.SocketBase workerSock : workerSocks) {
            ++idx;
            Thread workerThr = new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    try {
                        while (true) {
                            started.await();
                            zmq.Msg msg = workerSock.recv(0);

                            if (msg == null) {
                                break;
                            }
                            System.out.println(msg);
                            // Process the msg!
                            LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(10, TimeUnit.MILLISECONDS));
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
                }
            });
            workerThr.setName(String.format("W%d", idx));
            workerThr.start();
        }

        started.await(1000, TimeUnit.MILLISECONDS);
        Thread.sleep(sleep);

        System.out.print(CLOSING);

        zmq.ZMQ.close(recvMsgSock);
        zmq.ZMQ.close(processMsgSock);

        for (zmq.SocketBase workerSock : workerSocks) {
            zmq.ZMQ.close(workerSock);
        }

        zmq.ZMQ.term(ctx);

        stopped.await();
        System.out.print(CLOSED);
    }
}
