package org.zeromq.proxy;

import org.junit.Ignore;
import org.junit.Test;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

@Ignore
public class ProxiesStressTest
{
    @Test
    public void testPullPushPull() throws Exception
    {
        ProxyStressTester tester = new ProxyStressTester(zmq.ZMQ.ZMQ_PULL, zmq.ZMQ.ZMQ_PUSH, zmq.ZMQ.ZMQ_PULL);
        tester.testStress(zmq.ZMQ.class, 3, 200, 10);
        tester.testStress(ZMQ.Context.class, 5, 200, 10);
        tester.testStress(ZContext.class, 8, 200, 10);
    }

    @Test
    public void testPairPushPull() throws Exception
    {
        ProxyStressTester tester = new ProxyStressTester(zmq.ZMQ.ZMQ_PAIR, zmq.ZMQ.ZMQ_PUSH, zmq.ZMQ.ZMQ_PULL);
        tester.testStress(zmq.ZMQ.class, 5, 200, 10);
        tester.testStress(ZMQ.Context.class, 8, 200, 10);
        tester.testStress(ZContext.class, 3, 200, 10);
    }

    @Test
    public void testDealerRouterDealer() throws Exception
    {
        ProxyStressTester tester = new ProxyStressTester(zmq.ZMQ.ZMQ_DEALER, zmq.ZMQ.ZMQ_ROUTER, zmq.ZMQ.ZMQ_DEALER);
        tester.testStress(zmq.ZMQ.class, 8, 200, 10);
        tester.testStress(ZMQ.Context.class, 5, 200, 10);
        tester.testStress(ZContext.class, 3, 200, 10);
    }
}
