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

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

import zmq.ZError;

/**
 * In this test, we create sockets in the main thread, start a proxy with it, some workers,
 * and close all sockets in the main thread.
 */
public class StressTesterZeromq extends StressTesterZMQ
{
    public StressTesterZeromq(int frontEndType, int brokerType, int workerType) throws IOException
    {
        super("Zeromq", frontEndType, brokerType, workerType);
    }

    @SuppressWarnings("deprecation")
    @Override
    protected void performTest(int workers, long sleep)
            throws InterruptedException, BrokenBarrierException, TimeoutException
    {
        final ZContext ctx = new ZContext(1);
        final Socket recvMsgSock = ctx.createSocket(frontEndType);
        recvMsgSock.bind(frontendAddress);
        final Socket processMsgSock = ctx.createSocket(brokerType);
        processMsgSock.bind(PROXY_WORKERS);

        final CyclicBarrier started = new CyclicBarrier(workers + 1);
        final CountDownLatch stopped = new CountDownLatch(workers + 1);
        List<Socket> workerSockets = new ArrayList<Socket>();
        for (int i = 0; i < workers; i++) {
            Socket workerSock = ctx.createSocket(workerType);
            workerSock.connect(PROXY_WORKERS);
            workerSockets.add(workerSock);
        }

        Runnable runnable = new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    ZMQ.proxy(recvMsgSock, processMsgSock, null);
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
        for (final Socket workerSock : workerSockets) {
            ++idx;
            Thread workerThr = new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    try {
                        while (true) {
                            started.await();
                            ZMsg msg = ZMsg.recvMsg(workerSock);
                            if (msg == null) {
                                break;
                            }
                            System.out.println(msg);
                            LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(10, TimeUnit.MILLISECONDS));
                            // Process the msg!
                            LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(10, TimeUnit.MILLISECONDS));
                        }
                    }
                    catch (ZMQException e) {
                        // as the worker does not check is the context was terminated,
                        // and is not carefully closed
                        // expect a terminated exception
                        int errno = e.getErrorCode();
                        assert (errno == ZError.ETERM);
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
            workerThr.setName(String.format("Z%d", idx));
            workerThr.start();
        }

        started.await(100, TimeUnit.MILLISECONDS);
        Thread.sleep(sleep);

        System.out.print(CLOSING);

        recvMsgSock.close();
        processMsgSock.close();

        for (Socket workerSock : workerSockets) {
            workerSock.close();
        }

        ctx.close();

        stopped.await();
        System.out.print(CLOSED);
    }
}
