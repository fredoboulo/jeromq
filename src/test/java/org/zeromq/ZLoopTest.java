package org.zeromq;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zeromq.ZLoop.Handle;
import org.zeromq.ZLoop.IZLoopHandler;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;

public class ZLoopTest
{
    private String   received;
    private ZContext ctx;
    private Socket   input;
    private Socket   output;

    @Before
    public void setUp()
    {
        ctx = new ZContext();
        assertThat(ctx, notNullValue());

        output = ctx.createSocket(SocketType.PAIR);
        assertThat(output, notNullValue());
        output.bind("inproc://zloop.test");
        input = ctx.createSocket(SocketType.PAIR);
        assertThat(input, notNullValue());
        input.connect("inproc://zloop.test");

        received = "FAILED";
    }

    @After
    public void tearDown()
    {
        ctx.destroy();
    }

    @Test
    public void testReader()
    {
        int rc = 0;

        ZLoop loop = new ZLoop(ctx);
        assertThat(loop, notNullValue());

        ZLoop.IZLoopHandler<Socket> timerEvent = new ZLoop.IZLoopHandler<Socket>()
        {
            @Override
            public int handle(ZLoop loop, PollItem item, Socket arg)
            {
                arg.send("PING", 0);
                return 0;
            }
        };

        ZLoop.IZLoopHandler<Socket> socketEvent = new ZLoop.IZLoopHandler<Socket>()
        {
            @Override
            public int handle(ZLoop loop, PollItem item, Socket arg)
            {
                received = arg.recvStr(0);
                // Just end the reactor
                return -1;
            }
        };

        loop.verbose(true);

        //  Create a timer that will be cancelled
        Handle handle = loop.timer(1000, 1, timerEvent, input);
        assertThat(handle, notNullValue());
        handle = loop.timer(5, 1, new IZLoopHandler<Handle>()
        {

            @Override
            public int handle(ZLoop loop, PollItem item, Handle arg)
            {
                //  We are handling timer 2, and will cancel timer 1
                loop.removeTimer(arg);
                return 0;
            }
        }, handle);
        assertThat(handle, notNullValue());

        //  After 20 msecs, send a ping message to output3
        rc = loop.addTimer(20, 1, timerEvent, output);
        assertThat(rc, is(0));

        //  Set up some tickets that will never expire
        loop.ticketDelay(10000);
        Handle handle1 = loop.addTicket(timerEvent, null);
        assertThat(handle1, notNullValue());
        Handle handle2 = loop.addTicket(timerEvent, null);
        assertThat(handle2, notNullValue());
        Handle handle3 = loop.addTicket(timerEvent, null);
        assertThat(handle3, notNullValue());

        // When we get the ping message, end the reactor
        handle = loop.addReader(input, socketEvent, input);
        assertThat(handle, notNullValue());
        loop.setTolerantReader(handle);

        loop.start();

        loop.deleteTicket(handle1);
        loop.deleteTicket(handle2);
        loop.deleteTicket(handle3);

        //  Check whether loop properly ignores zsys_interrupted flag
        //  when asked to
        assertThat(received, is("PING"));
    }

    @Test
    public void testPoller()
    {
        int rc = 0;

        ZLoop loop = new ZLoop(ctx);
        assertThat(loop, notNullValue());

        ZLoop.IZLoopHandler<Socket> timerEvent = new ZLoop.IZLoopHandler<Socket>()
        {
            @Override
            public int handle(ZLoop loop, PollItem item, Socket arg)
            {
                arg.send("PING", 0);
                return 0;
            }
        };

        ZLoop.IZLoopHandler<Socket> socketEvent = new ZLoop.IZLoopHandler<Socket>()
        {
            @Override
            public int handle(ZLoop loop, PollItem item, Socket arg)
            {
                received = arg.recvStr(0);
                // Just end the reactor
                return -1;
            }
        };

        loop.verbose(true);

        //  After 20 msecs, send a ping message to output3
        rc = loop.addTimer(20, 1, timerEvent, output);
        assertThat(rc, is(0));

        // When we get the ping message, end the reactor
        PollItem pollInput = new PollItem(input, ZMQ.Poller.POLLIN);
        rc = loop.addPoller(pollInput, socketEvent, input);
        assertThat(rc, is(0));

        loop.start();

        loop.removePoller(pollInput);
        //  Check whether loop properly ignores zsys_interrupted flag
        //  when asked to
        assertThat(received, is("PING"));
    }

    @Test
    public void testZLoopAddTimerFromTimer()
    {
        int rc = 0;

        ZLoop loop = new ZLoop(ctx);
        assertThat(loop, notNullValue());

        ZLoop.IZLoopHandler<Socket> timerEvent = new ZLoop.IZLoopHandler<Socket>()
        {
            @Override
            public int handle(ZLoop loop, PollItem item, Socket arg)
            {
                final long now = System.currentTimeMillis();

                ZLoop.IZLoopHandler<Socket> timerEvent2 = new ZLoop.IZLoopHandler<Socket>()
                {
                    @Override
                    public int handle(ZLoop loop, PollItem item, Socket arg)
                    {
                        final long now2 = System.currentTimeMillis();
                        assertThat(now2 >= now + 10, is(true));
                        arg.send("PING", 0);
                        return 0;
                    }
                };
                loop.addTimer(10, 1, timerEvent2, arg);
                return 0;
            }
        };

        ZLoop.IZLoopHandler<Socket> socketEvent = new ZLoop.IZLoopHandler<Socket>()
        {
            @Override
            public int handle(ZLoop loop, PollItem item, Socket arg)
            {
                received = arg.recvStr(0);
                // Just end the reactor
                return -1;
            }
        };

        // After 10 msecs, fire a timer that registers
        // another timer that sends the ping message
        loop.addTimer(10, 1, timerEvent, input);

        // When we get the ping message, end the reactor
        PollItem pollInput = new PollItem(output, Poller.POLLIN);
        rc = loop.addPoller(pollInput, socketEvent, output);
        assertThat(rc, is(0));

        loop.start();

        loop.removePoller(pollInput);
        assertThat(received, is("PING"));

    }

    @Test(timeout = 1000)
    public void testZLoopAddTimerFromSocketHandler()
    {
        int rc = 0;

        ZLoop loop = new ZLoop(ctx);
        assertThat(loop, notNullValue());

        ZLoop.IZLoopHandler<Socket> timerEvent = new ZLoop.IZLoopHandler<Socket>()
        {
            @Override
            public int handle(ZLoop loop, PollItem item, Socket arg)
            {
                System.out.println("Send PING");
                arg.send("PING", 0);
                return 0;
            }
        };

        ZLoop.IZLoopHandler<Socket> socketEvent = new ZLoop.IZLoopHandler<Socket>()
        {
            @Override
            public int handle(ZLoop loop, PollItem item, Socket arg)
            {
                final long now = System.currentTimeMillis();
                ZLoop.IZLoopHandler<Socket> timerEvent2 = new ZLoop.IZLoopHandler<Socket>()
                {
                    @Override
                    public int handle(ZLoop loop, PollItem item, Socket arg)
                    {
                        System.out.println("Receive PING");
                        final long now2 = System.currentTimeMillis();
                        assertThat(now2 >= now + 10, is(true));
                        received = arg.recvStr(0);
                        // Just end the reactor
                        return -1;
                    }
                };
                System.out.println("Add Timer");
                // After 10 msec fire a timer that ends the reactor
                loop.addTimer(10, 1, timerEvent2, arg);
                return 0;
            }
        };

        // Fire a timer that sends the ping message
        loop.addTimer(0, 1, timerEvent, input);

        // When we get the ping message, end the reactor
        PollItem pollInput = new PollItem(output, Poller.POLLIN);
        rc = loop.addPoller(pollInput, socketEvent, output);
        assertThat(rc, is(0));

        loop.start();

        loop.removePoller(pollInput);
        assertThat(received, is("PING"));
    }

    @Test(timeout = 1000)
    public void testZLoopEndReactorFromTimer()
    {
        int rc = 0;

        ZLoop loop = new ZLoop(ctx);
        assertThat(loop, notNullValue());

        ZLoop.IZLoopHandler<Socket> timerEvent = new ZLoop.IZLoopHandler<Socket>()
        {
            @Override
            public int handle(ZLoop loop, PollItem item, Socket arg)
            {
                arg.send("PING", 0);
                return 0;
            }
        };

        ZLoop.IZLoopHandler<Socket> socketEvent = new ZLoop.IZLoopHandler<Socket>()
        {
            @Override
            public int handle(ZLoop loop, PollItem item, Socket arg)
            {
                // After 10 msecs, fire an event that ends the reactor
                ZLoop.IZLoopHandler<Socket> shutdownEvent = new ZLoop.IZLoopHandler<Socket>()
                {
                    @Override
                    public int handle(ZLoop loop, PollItem item, Socket arg)
                    {
                        received = arg.recvStr(0);
                        // Just end the reactor
                        return -1;
                    }
                };
                loop.addTimer(10, 1, shutdownEvent, arg);
                return 0;
            }
        };

        // Fire event that sends a ping message to output
        loop.addTimer(0, 1, timerEvent, input);

        // When we get the ping message, end the reactor
        PollItem pollInput = new PollItem(output, Poller.POLLIN);
        rc = loop.addPoller(pollInput, socketEvent, output);
        assertThat(rc, is(0));
        loop.start();

        loop.removePoller(pollInput);
        assertThat(received, is("PING"));
    }
}
