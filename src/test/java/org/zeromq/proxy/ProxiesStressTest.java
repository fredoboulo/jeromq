package org.zeromq.proxy;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class ProxiesStressTest
{
    private static final int SLEEP = 50;
    private static final int LOOPS = 500;

    @Test
    public void testPullPushPullZmq() throws Exception
    {
        new StressTesterZMQ(zmq.ZMQ.ZMQ_PULL, zmq.ZMQ.ZMQ_PUSH, zmq.ZMQ.ZMQ_PULL).testStress(3, SLEEP, LOOPS);
    }

    @Test
    public void testPullPushPullJeromq() throws Exception
    {
        new StressTesterJeromq(zmq.ZMQ.ZMQ_PULL, zmq.ZMQ.ZMQ_PUSH, zmq.ZMQ.ZMQ_PULL).testStress(5, SLEEP, LOOPS);
    }

    @Test
    public void testPullPushPullZeromq() throws Exception
    {
        new StressTesterZeromq(zmq.ZMQ.ZMQ_PULL, zmq.ZMQ.ZMQ_PUSH, zmq.ZMQ.ZMQ_PULL).testStress(8, SLEEP, LOOPS);
    }

    @Test
    public void testPairPushPullZmq() throws Exception
    {
        new StressTesterZMQ(zmq.ZMQ.ZMQ_PAIR, zmq.ZMQ.ZMQ_PUSH, zmq.ZMQ.ZMQ_PULL).testStress(5, SLEEP, LOOPS);
    }

    @Test
    public void testPairPushPullJeromq() throws Exception
    {
        new StressTesterJeromq(zmq.ZMQ.ZMQ_PAIR, zmq.ZMQ.ZMQ_PUSH, zmq.ZMQ.ZMQ_PULL).testStress(8, SLEEP, LOOPS);
    }

    @Test
    public void testPairPushPullZeromq() throws Exception
    {
        new StressTesterZeromq(zmq.ZMQ.ZMQ_PAIR, zmq.ZMQ.ZMQ_PUSH, zmq.ZMQ.ZMQ_PULL).testStress(3, SLEEP, LOOPS);
    }

    @Test
    public void testDealerRouterDealerZmq() throws Exception
    {
        new StressTesterZMQ(zmq.ZMQ.ZMQ_DEALER, zmq.ZMQ.ZMQ_ROUTER, zmq.ZMQ.ZMQ_DEALER).testStress(8, SLEEP, LOOPS);
    }

    @Test
    public void testDealerRouterDealerJeromq() throws Exception
    {
        new StressTesterJeromq(zmq.ZMQ.ZMQ_DEALER, zmq.ZMQ.ZMQ_ROUTER, zmq.ZMQ.ZMQ_DEALER).testStress(5, SLEEP, LOOPS);
    }

    @Test
    public void testDealerRouterDealerZeromq() throws Exception
    {
        new StressTesterZeromq(zmq.ZMQ.ZMQ_DEALER, zmq.ZMQ.ZMQ_ROUTER, zmq.ZMQ.ZMQ_DEALER).testStress(3, SLEEP, LOOPS);
    }
}
