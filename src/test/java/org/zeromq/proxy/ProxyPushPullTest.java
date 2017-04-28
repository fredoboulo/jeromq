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

import org.junit.Ignore;
import org.junit.Test;
import org.zeromq.Utils;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZMQQueue;
import org.zeromq.ZMsg;

import zmq.Helper;
import zmq.ZError;

/**
 * In this test, we create sockets in the main thread, start a proxy with it, some workers,
 * and close all sockets in the main thread.
 *
 */
@Ignore
public class ProxyPushPullTest
{
    private static final String FRONTEND       = "tcp://*:";
    private static final String PROXY_WORKERS  = "inproc://process-msg";
    private static final String WORKER_PATTERN = " %s ";
    private static final String PROXY          = "Proxy";
    private static final String PROXY_THREAD   = PROXY;
    private static final String CLOSED         = "... Closed.";
    private static final String CLOSING        = "Closing ...";

    @Test
    public void testStressProxyZMQUnit() throws Exception
    {
        int workers = 1;
        int sleep = 30 * workers;
        int loops = 500;
        for (int i = 0; i < loops; i++) {
            if (i > 0) {
                System.out.printf(Helper.rewind(72 + 7 + 4 * workers));
            }
            System.out.printf("Test  U ZMQ #%04d starting ", i);
            long start = System.currentTimeMillis();
            performTestZmq(workers, sleep);
            long end = System.currentTimeMillis();
            System.out.printf(" performed in %3d millis.", (end - start));
        }
        System.out.println();
    }

    @Test
    public void testStressProxyZMQ() throws Exception
    {
        int workers = 3;
        int sleep = 30 * workers + 30;
        int loops = 100;
        for (int i = 0; i < loops; i++) {
            if (i > 0) {
                System.out.printf(Helper.rewind(72 + 7 + 4 * workers));
            }
            System.out.printf("Test    ZMQ #%04d starting ", i);
            long start = System.currentTimeMillis();
            performTestZmq(workers, sleep);
            long end = System.currentTimeMillis();
            System.out.printf(" performed in %3d millis.", (end - start));
        }
        System.out.println();
    }

    @Test
    public void testStressProxyJeromq() throws Exception
    {
        int workers = 9;
        int sleep = 30 * workers + 30;
        int loops = 2;
        for (int i = 0; i < loops; i++) {
            if (i > 0) {
                System.out.printf(Helper.rewind(72 + 7 + 4 * workers));
            }
            System.out.printf("Test Jeromq #%04d starting ", i);
            long start = System.currentTimeMillis();
            performTestJeromq(workers, sleep);
            long end = System.currentTimeMillis();
            System.out.printf(" performed in %3d millis.", (end - start));
        }
        System.out.println();
    }

    @Test
    public void testStressProxyZeromq() throws Exception
    {
        int workers = 5;
        int sleep = 30 * workers + 100;
        int loops = 50;
        for (int i = 0; i < loops; i++) {
            if (i > 0) {
                System.out.printf(Helper.rewind(72 + 7 + 4 * workers));
            }
            System.out.printf("Test Zeromq #%04d starting ", i);
            long start = System.currentTimeMillis();
            performTestZeromq(workers, sleep);
            long end = System.currentTimeMillis();
            System.out.printf(" performed in %3d millis.", (end - start));
        }
        System.out.println();
    }

    void performTestZmq(int workers, long sleep)
            throws InterruptedException, BrokenBarrierException, TimeoutException, IOException
    {
        final zmq.Ctx ctx = zmq.ZMQ.createContext();
        final zmq.SocketBase recvMsgSock = zmq.ZMQ.socket(ctx, zmq.ZMQ.ZMQ_PULL);
        recvMsgSock.bind(FRONTEND + Utils.findOpenPort());
        final zmq.SocketBase processMsgSock = zmq.ZMQ.socket(ctx, zmq.ZMQ.ZMQ_PUSH);
        processMsgSock.bind(PROXY_WORKERS);

        final CountDownLatch stopped = new CountDownLatch(workers + 1);
        final CyclicBarrier started = new CyclicBarrier(workers + 1);
        List<zmq.SocketBase> workerSocks = new ArrayList<zmq.SocketBase>();
        for (int i = 0; i < workers; i++) {
            zmq.SocketBase workerSock = zmq.ZMQ.socket(ctx, zmq.ZMQ.ZMQ_PULL);
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

    void performTestJeromq(int workers, long sleep)
            throws InterruptedException, BrokenBarrierException, TimeoutException, IOException
    {
        final Context ctx = ZMQ.context(1);
        final Socket recvMsgSock = ctx.socket(ZMQ.PULL);
        recvMsgSock.bind(FRONTEND + Utils.findOpenPort());
        final Socket processMsgSock = ctx.socket(ZMQ.PUSH);
        processMsgSock.bind(PROXY_WORKERS);

        final CyclicBarrier started = new CyclicBarrier(workers + 1);
        final CountDownLatch stopped = new CountDownLatch(workers + 1);
        List<Socket> workerSocks = new ArrayList<Socket>();
        for (int i = 0; i < workers; i++) {
            Socket workerSock = ctx.socket(ZMQ.PULL);
            workerSock.connect(PROXY_WORKERS);
            workerSocks.add(workerSock);
        }

        Runnable runnable = new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    ZMQQueue queue = new ZMQQueue(ctx, recvMsgSock, processMsgSock);
                    queue.run();
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
        for (final Socket workerSock : workerSocks) {
            ++idx;
            Thread workerThr = new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    try {
                        while (true) {
                            started.await();
                            byte[] msg = workerSock.recv();

                            if (msg == null) {
                                break;
                            }
                            System.out.println(msg);
                            LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(10, TimeUnit.MILLISECONDS));
                        }
                    }
                    catch (ZMQException e) {
                        // as the worker does not check is the context was terminated,
                        // and is not carefully closed
                        // expect a terminated exception
                        int errno = e.getErrorCode();
                        assert (errno == zmq.ZError.ETERM);
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
            workerThr.setName(String.format("X%d", idx));
            workerThr.start();
        }

        started.await(1000, TimeUnit.MILLISECONDS);
        Thread.sleep(sleep);

        System.out.print(CLOSING);

        recvMsgSock.close();
        processMsgSock.close();

        for (Socket workerSock : workerSocks) {
            workerSock.close();
        }

        ctx.term();

        stopped.await();
        System.out.print(CLOSED);
    }

    void performTestZeromq(int workers, long sleep)
            throws InterruptedException, BrokenBarrierException, TimeoutException, IOException
    {
        final ZContext ctx = new ZContext(1);
        final Socket recvMsgSock = ctx.createSocket(ZMQ.PULL);
        recvMsgSock.bind(FRONTEND + Utils.findOpenPort());
        final Socket processMsgSock = ctx.createSocket(ZMQ.PUSH);
        processMsgSock.bind(PROXY_WORKERS);

        final CyclicBarrier started = new CyclicBarrier(workers + 1);
        final CountDownLatch stopped = new CountDownLatch(workers + 1);
        List<Socket> workerSockets = new ArrayList<Socket>();
        for (int i = 0; i < workers; i++) {
            Socket workerSock = ctx.createSocket(ZMQ.PULL);
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
        LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(sleep, TimeUnit.MILLISECONDS));

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
