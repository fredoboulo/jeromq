package org.zeromq.proxy;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import zmq.Helper;

/**
 * In this test, we create sockets in the main thread, start a proxy with it, some workers,
 * and close all sockets in the main thread.
 *
 */
public class ProxyStressTester
{
    private final int frontEndType;
    private final int brokerType;
    private final int workerType;

    public ProxyStressTester(int frontEndType, int brokerType, int workerType)
    {
        super();
        this.frontEndType = frontEndType;
        this.brokerType = brokerType;
        this.workerType = workerType;
    }

    public void testStressProxyZMQ() throws Exception
    {
        int workers = 3;
        int sleep = 20;
        int loops = 500;
        testStress(zmq.ZMQ.class, workers, sleep, loops);
    }

    public void testStressProxyJeromq() throws Exception
    {
        int workers = 9;
        int sleep = 50;
        int loops = 100;
        testStress(ZMQ.Context.class, workers, sleep, loops);
    }

    public void testStressProxyZeromq() throws Exception
    {
        int workers = 5;
        int sleep = 50;
        int loops = 200;
        testStress(ZContext.class, workers, sleep, loops);
    }

    public void testStress(Class<?> clazz, int workers, int sleep, int loops) throws Exception
    {
        String print = null;
        int type = 0; // 0: ZMQ 1: Jeromq 2: Zeromq
        String label = "   ZMQ";
        if (zmq.ZMQ.class.equals(clazz)) {
            //
        }
        if (ZMQ.Context.class.equals(clazz)) {
            type = 1;
            label = "Jeromq";
        }
        if (ZContext.class.equals(clazz)) {
            type = 2;
            label = "Zeromq";
        }
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
            switch (type) {
            case 0:
                new StressTesterZMQ(frontEndType, brokerType, workerType).performTest(workers, sleep);
                break;
            case 1:
                new StressTesterJeromq(frontEndType, brokerType, workerType).performTest(workers, sleep);
                break;
            case 2:
                new StressTesterZeromq(frontEndType, brokerType, workerType).performTest(workers, sleep);
                break;

            default:
                break;
            }
            long end = System.currentTimeMillis();
            String footer = String.format(" performed in %3d millis.", (end - start));
            System.out.printf(footer);
            print += footer;
        }
        System.out.println();
    }
}
