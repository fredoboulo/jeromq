package org.zeromq.proxy;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Ignore;
import org.junit.Test;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

@Ignore
public class ZmqXpubXsubTester
{
    // Publishers *bind* to this socket, the SUB of the proxy
    static String frontend = "tcp://localhost:8010";

    // Subscribers *connect* to this socket, the PUB of the proxy
    static String backend = "tcp://localhost:8011";

    static AtomicInteger sendNumber = new AtomicInteger();

    static AtomicInteger receiveNumber = new AtomicInteger();

    @Test
    public void testXpubXsub() throws Exception
    {
        //
        // PROXY
        //
        Runnable proxy = createProxy();
        Thread tProxy = new Thread(proxy);
        tProxy.setName("PROXY");
        tProxy.start();

        Thread.sleep(1000);

        //
        // Subscribers
        //
        for (int i = 0; i < 1; i++) {
            Thread tSubscriber = new Thread(createSubscriber());
            tSubscriber.setName("Subscriber-" + i);
            tSubscriber.start();
        }

        Thread.sleep(1000);

        //
        // Publishers
        //
        for (int i = 0; i < 1; i++) {
            Thread tPublisher = new Thread(createPublisher());
            tPublisher.setName("Publisher-" + i);
        }

        Thread.sleep(30000);
    }

    private Runnable createProxy()
    {
        Runnable proxy = new Runnable()
        {
            @Override
            public void run()
            {
                // Create XPUB/XSUB Proxy
                ZContext ctx = new ZContext();
                Socket xpub = ctx.createSocket(ZMQ.XPUB);
                xpub.bind(backend);
                Socket xsub = ctx.createSocket(ZMQ.XSUB);
                xpub.bind(frontend);

                // Call blocks
                ZMQ.proxy(xpub, xsub, null);
                System.out.println("PROXY FINISHED !?");
            }
        };
        return proxy;
    }

    private Runnable createPublisher()
    {
        Runnable rv = new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    ZContext ctx = new ZContext();
                    Socket pub = ctx.createSocket(ZMQ.PUB);
                    pub.connect(frontend);
                    System.out.println("Publisher started");
                    Thread.sleep(2000);

                    for (int i = 0; i < 100; i++) {
                        int m = sendNumber.incrementAndGet();
                        ZMsg zmsg = ZMsg.newStringMsg("test", new String("Message Number " + m));
                        boolean rv = zmsg.send(pub);
                        System.out.println("Sent msg #" + m + "; rv = " + rv);
                        Thread.sleep(100);
                    }
                    pub.close();
                    ctx.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("Publisher startup with exception: " + e.getMessage());
                }
            }
        };
        return rv;
    }

    private Runnable createSubscriber()
    {
        Runnable rv = new Runnable()
        {
            @Override
            public void run()
            {
                try (
                     ZContext ctx = new ZContext()) {
                    Socket sub = ctx.createSocket(ZMQ.SUB);
                    sub.connect(backend);
                    sub.subscribe(ZMQ.SUBSCRIPTION_ALL);
                    //                    sub.setRcvHWM(10000);
                    System.out.println("Subscriber started");
                    while (true) {
                        ZMsg message = ZMsg.recvMsg(sub);
                        int m = receiveNumber.incrementAndGet();
                        System.out.println("msg receivd: " + new String(message.popString()) + ", counter now #" + m);
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("Subscriber startup with exception: " + e.getMessage());
                }
            }
        };
        return rv;
    }
}
