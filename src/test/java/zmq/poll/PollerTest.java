package zmq.poll;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.nio.channels.Selector;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import zmq.Ctx;
import zmq.ZMQ;
import zmq.poll.Poller.Handle;
import zmq.util.ValueReference;

public class PollerTest
{
    private final class PollEventsImplementation implements IPollEvents
    {
        private final AtomicInteger ins  = new AtomicInteger();
        private final AtomicInteger outs = new AtomicInteger();

        @Override
        public void inEvent()
        {
            ins.incrementAndGet();
        }

        @Override
        public void outEvent()
        {
            outs.incrementAndGet();
        }

        @Override
        public void timerEvent(int id)
        {
        }
    }

    private static Ctx ctx;

    @BeforeClass
    public static void setUp()
    {
        ctx = ZMQ.createContext();
    }

    @AfterClass
    public static void tearDown()
    {
        ZMQ.term(ctx);
    }

    @Test
    public void testDoNotTriggerCounter()
    {
        final Poller poller = new Poller(ctx, "test");

        final int maybeRebuildSelector = poller.maybeRebuildSelector(new ValueReference<>(0), 10, 20);
        assertThat(maybeRebuildSelector, is(0));
    }

    @Test
    public void testDoNotTriggerCounterAndReturnZero()
    {
        final Poller poller = new Poller(ctx, "test");
        final int maybeRebuildSelector = poller.maybeRebuildSelector(new ValueReference<>(5), 10, 20);
        assertThat(maybeRebuildSelector, is(0));
    }

    @Test
    public void testTriggerCounter()
    {
        final Poller poller = new Poller(ctx, "test");
        final int maybeRebuildSelector = poller.maybeRebuildSelector(new ValueReference<>(1), 20, 5);
        assertThat(maybeRebuildSelector, is(2));
    }

    @Test
    public void testNotRebuildSelector()
    {
        final Poller poller = new Poller(ctx, "test");
        final Selector old = poller.selector();
        final int maybeRebuildSelector = poller.maybeRebuildSelector(new ValueReference<>(9), 20, 5);
        assertThat(maybeRebuildSelector, is(10));
        final Selector current = poller.selector();
        assertThat(current, is(old));
        assertThat(old.isOpen(), is(true));
    }

    @Test
    public void testRebuildSelector()
    {
        final Poller poller = new Poller(ctx, "test");
        final Selector old = poller.selector();
        final int maybeRebuildSelector = poller.maybeRebuildSelector(new ValueReference<>(10), 20, 5);
        assertThat(maybeRebuildSelector, is(0));
        final Selector created = poller.selector();
        assertThat(created, is(not(old)));
        assertThat(old.isOpen(), is(false));
    }

    @Test
    public void testTriggerRebuildSelector() throws IOException
    {
        final Poller poller = new Poller(ctx, "test", w -> true);

        final Selector old = poller.selector();
        final Pipe pipe = Pipe.open();
        final IPollEvents events = new PollEventsImplementation();
        final Handle handle = poller.addHandle(pipe.source(), events);
        poller.setPollIn(handle);

        pipe.source().configureBlocking(false);
        final ValueReference<Integer> returnsImmediately = new ValueReference<>(0);
        for (int idx = 0; idx < 10; ++idx) {
            old.wakeup();
            final boolean dispatched = poller.run(returnsImmediately);
            assertThat(dispatched, is(false));
            assertThat(returnsImmediately.get(), is(idx + 1));
        }
        // this one will trigger the rebuilding of the selector
        old.wakeup();
        final boolean dispatched = poller.run(returnsImmediately);
        assertThat(dispatched, is(false));
        assertThat(returnsImmediately.get(), is(0));
        assertThat(poller.selector(), is(not(old)));
    }

    @Test
    public void testTriggerResetSelector() throws IOException
    {
        final Poller poller = new Poller(ctx, "test", w -> true);

        final Selector old = poller.selector();
        final Pipe pipe = Pipe.open();
        final IPollEvents events = new PollEventsImplementation();
        final Handle handle = poller.addHandle(pipe.source(), events);
        poller.setPollIn(handle);

        pipe.source().configureBlocking(false);
        final ValueReference<Integer> returnsImmediately = new ValueReference<>(0);
        for (int idx = 0; idx < 5; ++idx) {
            old.wakeup();
            final boolean dispatched = poller.run(returnsImmediately);
            assertThat(dispatched, is(false));
            assertThat(returnsImmediately.get(), is(idx + 1));
        }
        // this one will trigger the rebuilding of the selector
        pipe.sink().write(ByteBuffer.allocate(5));
        final boolean dispatched = poller.run(returnsImmediately);
        assertThat(dispatched, is(true));
        assertThat(returnsImmediately.get(), is(0));
        assertThat(poller.selector(), is(old));
    }
}
