package org.zeromq.proxy;

import org.junit.Test;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class ProxiesStressTest
{
    @Test
    public void testPullPushPullZmq() throws Exception
    {
        ProxyStressTester tester = new ProxyStressTester(zmq.ZMQ.ZMQ_PULL, zmq.ZMQ.ZMQ_PUSH, zmq.ZMQ.ZMQ_PULL);
        tester.testStress(zmq.ZMQ.class, 3, 100, 30);
    }

    @Test
    public void testPullPushPullJeromq() throws Exception
    {
        ProxyStressTester tester = new ProxyStressTester(zmq.ZMQ.ZMQ_PULL, zmq.ZMQ.ZMQ_PUSH, zmq.ZMQ.ZMQ_PULL);
        tester.testStress(ZMQ.Context.class, 5, 100, 30);
    }

    @Test
    public void testPullPushPullZeromq() throws Exception
    {
        ProxyStressTester tester = new ProxyStressTester(zmq.ZMQ.ZMQ_PULL, zmq.ZMQ.ZMQ_PUSH, zmq.ZMQ.ZMQ_PULL);
        tester.testStress(ZContext.class, 8, 100, 30);
    }

    @Test
    public void testPairPushPullZmq() throws Exception
    {
        ProxyStressTester tester = new ProxyStressTester(zmq.ZMQ.ZMQ_PAIR, zmq.ZMQ.ZMQ_PUSH, zmq.ZMQ.ZMQ_PULL);
        tester.testStress(zmq.ZMQ.class, 5, 100, 30);
    }

    @Test
    public void testPairPushPullJeromq() throws Exception
    {
        ProxyStressTester tester = new ProxyStressTester(zmq.ZMQ.ZMQ_PAIR, zmq.ZMQ.ZMQ_PUSH, zmq.ZMQ.ZMQ_PULL);
        tester.testStress(ZMQ.Context.class, 8, 100, 30);
    }

    @Test
    public void testPairPushPullZeromq() throws Exception
    {
        ProxyStressTester tester = new ProxyStressTester(zmq.ZMQ.ZMQ_PAIR, zmq.ZMQ.ZMQ_PUSH, zmq.ZMQ.ZMQ_PULL);
        tester.testStress(ZContext.class, 3, 100, 30);
    }

    @Test
    public void testDealerRouterDealerZmq() throws Exception
    {
        ProxyStressTester tester = new ProxyStressTester(zmq.ZMQ.ZMQ_DEALER, zmq.ZMQ.ZMQ_ROUTER, zmq.ZMQ.ZMQ_DEALER);
        tester.testStress(zmq.ZMQ.class, 8, 100, 30);
    }

    @Test
    public void testDealerRouterDealerJeromq() throws Exception
    {
        ProxyStressTester tester = new ProxyStressTester(zmq.ZMQ.ZMQ_DEALER, zmq.ZMQ.ZMQ_ROUTER, zmq.ZMQ.ZMQ_DEALER);
        tester.testStress(ZMQ.Context.class, 5, 100, 30);
    }

    @Test
    public void testDealerRouterDealerZeromq() throws Exception
    {
        ProxyStressTester tester = new ProxyStressTester(zmq.ZMQ.ZMQ_DEALER, zmq.ZMQ.ZMQ_ROUTER, zmq.ZMQ.ZMQ_DEALER);
        tester.testStress(ZContext.class, 3, 100, 30);
    }
}
