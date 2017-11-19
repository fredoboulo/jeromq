package perf;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Exchanger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;

import zmq.Helper;

/**
 * Tests a REQ-ROUTER dialog with several methods,
 * each component being on a separate thread.
 * @author fred
 *
 */
public class TestPerfs
{
    private static final String WAIT          = " ** Wait ** ";
    private static final String WARNING       = "    /!\\     ";
    private static final int    POWER_MESSAGE = 6;              // 6
    private static final int    POWER_PAYLOAD = 8;              // 8
    private static final int    LOOPS_10      = 10;

    private static final int LOOPS_2         = 25;
    private static final int POWER_2_MESSAGE = 9; // 9
    private static final int POWER_2_PAYLOAD = 9; // 9

    private static MetricRegistry registry;

    private static final Map<String, Long> times = new HashMap<String, Long>();

    @BeforeClass
    public static void beforeClass()
    {
        registry = new MetricRegistry();
    }

    @AfterClass
    public static void afterClass()
    {
        try {
            new MetricReader().print(registry, times);
            // display the number of signals and selectors left opened after each test.
        }
        catch (Throwable t) {
            t.printStackTrace();
        }
    }

    @Test
    public void testPerfRepeated() throws Exception
    {
        System.out.printf("\nTest Perfs Repeated %s times ", LOOPS_10);
        StringBuilder builder = new StringBuilder();
        builder.append(" on a matrix [");
        for (int powerMsg = POWER_MESSAGE + 1; powerMsg > 0; powerMsg--) {
            int message = (int) Math.pow(10, powerMsg);
            format(builder, message);
            if (powerMsg > 0) {
                builder.append(", ");
            }
        }
        builder.append("] messages x size ");
        builder.append("[");
        for (int powerPayload = POWER_PAYLOAD + 1; powerPayload > 0; powerPayload--) {
            int payload = (int) Math.pow(10, powerPayload);
            format(builder, payload);
            if (powerPayload > 0) {
                builder.append(", ");
            }
        }
        builder.append("]");
        System.out.println(builder.toString());
        long start = System.currentTimeMillis();

        for (int index = 0; index < LOOPS_10; ++index) {
            System.out.print(index % 10);
            test(index, POWER_PAYLOAD + 1, POWER_MESSAGE + 1, registry, 1);
            if (index % 10 == 0) {
                System.out.print("\b" + index);
            }
            else if (index % 3 == 0) {
                System.out.print("\b*");
            }
            else {
                System.out.print("\b~");
            }
        }
        long end = System.currentTimeMillis();
        times.put("Multiples of 10", end - start);
    }

    @Test
    public void testPerfRepeated2() throws Exception
    {
        System.out.printf("\nTest Perfs 2 Repeated %s times ", LOOPS_10 * 4);
        StringBuilder builder = new StringBuilder();
        builder.append(" on a matrix [");
        for (int powerMsg = POWER_MESSAGE; powerMsg > 0; powerMsg--) {
            int message = (int) Math.pow(10, powerMsg) * 5;
            format(builder, message);
            if (powerMsg > 0) {
                builder.append(", ");
            }
        }
        builder.append("] messages x size ");
        builder.append("[");
        for (int powerPayload = POWER_PAYLOAD; powerPayload > 0; powerPayload--) {
            int payload = (int) Math.pow(10, powerPayload) * 5;
            format(builder, payload);
            if (powerPayload > 0) {
                builder.append(", ");
            }
        }
        builder.append("]");
        System.out.println(builder.toString());
        long start = System.currentTimeMillis();

        for (int index = 0; index < LOOPS_10 * 4; ++index) {
            System.out.print(index % 10);
            test(index, POWER_PAYLOAD, POWER_MESSAGE, registry, 5);
            if (index % 10 == 0) {
                System.out.print("\b" + index);
            }
            else if (index % 3 == 0) {
                System.out.print("\b*");
            }
            else {
                System.out.print("\b~");
            }
        }
        long end = System.currentTimeMillis();
        times.put("Multiples of 10", end - start);
    }

    @Test
    public void testPerfRepeatedPower2() throws Exception
    {
        System.out.printf("\nTest Perfs Power of 2 Repeated %s times ", LOOPS_2);
        StringBuilder builder = new StringBuilder();
        builder.append(" on a matrix [");
        for (int powerMsg = POWER_2_MESSAGE + 1; powerMsg > 0; powerMsg--) {
            int message = 2 << powerMsg;
            format(builder, message);
            if (powerMsg > 0) {
                builder.append(", ");
            }
        }
        builder.append("] messages x size ");
        builder.append("[");
        for (int powerPayload = POWER_2_PAYLOAD + 1; powerPayload > 0; powerPayload--) {
            int payload = 2 << powerPayload;
            format(builder, payload);
            if (powerPayload > 0) {
                builder.append(", ");
            }
        }
        builder.append("]");
        System.out.println(builder.toString());
        long start = System.currentTimeMillis();

        for (int index = 0; index < LOOPS_2; ++index) {
            System.out.print(index % 10);

            test2(index, registry);

            if (index % 10 == 0) {
                System.out.print("\b" + index);
            }
            else if (index % 3 == 0) {
                System.out.print("\b*");
            }
            else {
                System.out.print("\b~");
            }
        }
        long end = System.currentTimeMillis();
        times.put("Multiples of 2", end - start);
    }

    @Ignore
    @Test
    public void testPerfRepeated100to1000() throws Exception
    {
        int increment = 128;
        int startM = 128;

        int loops = LOOPS_2;
        System.out.printf("\nTest Perfs 100-1k Repeated %s times ", loops);
        test("100-1k", startM, increment, loops);
    }

    @Test
    public void testPerfRepeated1M1() throws Exception
    {
        int loops = 2 * LOOPS_2 / 3;
        System.out.printf("\nTest Perfs 1M msgs of 1 byte Repeated %s times ", loops);
        test4(-1, "1b / 1M", 1, 0, 1000000, 0, loops, false);
    }

    @Test
    public void testPerfRepeated1M10() throws Exception
    {
        int loops = 2 * LOOPS_2 / 3;
        System.out.printf("\nTest Perfs 1M msgs of 10 bytes Repeated %s times ", loops);
        test4(-1, "10b / 1M", 10, 0, 1000000, 0, loops, false);
    }

    @Test
    public void testPerfRepeated1M100() throws Exception
    {
        int loops = 2 * LOOPS_2 / 3;
        System.out.printf("\nTest Perfs 1M msgs of 100 bytes Repeated %s times ", loops);
        test4(-1, "100b / 1M", 100, 0, 1000000, 0, loops, false);
    }

    @Test
    public void testPerfRepeated1M1K() throws Exception
    {
        int loops = 2 * LOOPS_2 / 3;
        System.out.printf("\nTest Perfs 1M msgs of 1K bytes Repeated %s times ", loops);
        test4(-1, "1K / 1M", 1000, 0, 1000000, 0, loops, false);
    }

    @Ignore
    @Test
    public void testPerfRepeated1M10K() throws Exception
    {
        int loops = 2 * LOOPS_2 / 3;
        System.out.printf("\nTest Perfs 1M msgs of 10K bytes Repeated %s times ", loops);
        test4(-1, "10K / 1M", 10000, 0, 1000000, 0, loops, false);
    }

    @Test
    public void testPerfRepeated200K1() throws Exception
    {
        int loops = 2 * LOOPS_2 / 3;
        System.out.printf("\nTest Perfs 200K msgs of 1 byte Repeated %s times ", loops);
        test4(-1, "200K msgs 1 byte", 1, 0, 200 * 1000, 0, loops, false);
    }

    @Test
    public void testPerfRepeated250K1() throws Exception
    {
        int loops = 2 * LOOPS_2 / 3;
        System.out.printf("\nTest Perfs 250K msgs of 1 byte Repeated %s times ", loops);
        test4(-1, "250K msgs 1 byte ", 1, 0, 250 * 1000, 0, loops, false);
    }

    @Test
    public void testPerfRepeated1200K() throws Exception
    {
        int loops = 2 * LOOPS_2 / 3;
        System.out.printf("\nTest Perfs 1 msg of 200K bytes Repeated %s times ", loops);
        test4(-1, "1 msg of 200K bytes", 200 * 1000, 0, 1, 0, loops, false);
    }

    @Test
    public void testPerfRepeated1250K() throws Exception
    {
        int loops = 2 * LOOPS_2 / 3;
        System.out.printf("\nTest Perfs 1 msg of 200K bytes Repeated %s times ", loops);
        test4(-1, "1 msg of 250K bytes ", 250 * 1000, 0, 1, 0, loops, false);
    }

    @Test
    public void testPerfRepeated110M() throws Exception
    {
        int loops = 2 * LOOPS_2 / 3;
        System.out.printf("\nTest Perfs 10Mb / 1 msg Repeated %s times ", loops);
        test4(-1, "10Mb / 1 msg", 10 * 1000 * 1000, 0, 1, 0, loops, false);
    }

    @Test
    public void testPerfRepeated150M() throws Exception
    {
        int loops = 2 * LOOPS_2 / 3;
        System.out.printf("\nTest Perfs 50Mb / 1 msg Repeated %s times ", loops);
        test4(-1, "50Mb / 1 msg", 50 * 1000 * 1000, 0, 1, 0, loops, false);
    }

    @Test
    public void testPerfRepeated100to1000Cross() throws Exception
    {
        int loops = LOOPS_2 / 2;
        System.out.printf("\nTest Perfs 100-1k Small steps Repeated %s times ", loops);
        test4("100-1k-cross", 200, 200, 100, 100, loops, false);
    }

    @Test
    public void testPerfRepeated100to1000Diag() throws Exception
    {
        int loops = LOOPS_2 * 4;
        System.out.printf("\nTest Perfs 200-2k Small steps Diag Repeated %s times ", loops);
        test4("200-2k-diag", 0, 200, 100, 100, loops, true);
    }

    @Test
    public void testPerfRepeated256to1024Slop() throws Exception
    {
        int loops = LOOPS_2 * 4;
        System.out.printf("\nTest Perfs 256-1024 Small steps Diag Repeated %s times ", loops);
        test4("256-1024-diag", 0, 256, 100, 100, loops, true);
    }

    @Ignore
    @Test
    public void testPerfRepeated1000to3000Cross() throws Exception
    {
        int loops = LOOPS_2;
        System.out.printf("\nTest Perfs 1k-3k Small steps Repeated %s times ", loops);
        test4("1k-3k-cross", 20, 20, 1000, 512, loops, false);
    }

    @Ignore
    @Test
    public void testPerfRepeated1000to3000Cross2() throws Exception
    {
        int loops = LOOPS_2;
        System.out.printf("\nTest Perfs 1k-3k Small steps Repeated %s times ", loops);
        test4("1k-3k-cross 2", 20, 20, 100, 100, loops, false);
    }

    @Ignore
    @Test
    public void testPerfRepeated1000to3000Cross3() throws Exception
    {
        int loops = LOOPS_2;
        System.out.printf("\nTest Perfs 1k-3k Small steps Repeated %s times ", loops);
        test4("1k-3k-cross 3", 100, 100, 1000, 512, loops, false);
    }

    private void test(String label, int startM, int increment, int loops) throws Exception
    {
        StringBuilder builder = new StringBuilder();
        builder.append(" on a matrix [");
        for (int powerMsg = POWER_2_MESSAGE + 1; powerMsg > 0; powerMsg--) {
            int message = startM + increment * powerMsg;
            format(builder, message);
            if (powerMsg > 0) {
                builder.append(", ");
            }
        }
        builder.append("] messages x size ");
        builder.append("[");
        for (int i = POWER_2_PAYLOAD + 1; i > 0; i--) {
            int payload = 2 << i;
            format(builder, payload);
            if (i > 0) {
                builder.append(", ");
            }
        }
        builder.append("]");
        System.out.println(builder.toString());
        long start = System.currentTimeMillis();

        for (int index = 0; index < loops; ++index) {
            System.out.print(index % 10);

            test3(index, startM, increment, registry);

            if (index % 10 == 0) {
                System.out.print("\b" + index);
            }
            else if (index % 3 == 0) {
                System.out.print("\b*");
            }
            else {
                System.out.print("\b~");
            }
        }
        long end = System.currentTimeMillis();
        times.put(label, end - start);
    }

    private void test4(String label, int starts, int incs, int startM, int incrementm, int loops, boolean slop)
            throws Exception
    {
        test4(10, label, starts, incs, startM, incrementm, loops, slop);
    }

    private void test4(int count, String label, int starts, int incs, int startM, int incrementm, int loops,
                       boolean slop)
            throws Exception
    {
        StringBuilder builder = new StringBuilder();
        if (count < 0) {
            count = -count;
        }
        else {
            builder.append(" on a matrix [");
            for (int powerMsg = POWER_2_MESSAGE + 1; powerMsg > 0; powerMsg--) {
                int message = startM + incrementm * powerMsg;
                format(builder, message);
                if (powerMsg > 0) {
                    builder.append(", ");
                }
            }
            builder.append("] messages x size ");
            builder.append("[");
            for (int i = POWER_2_PAYLOAD + 1; i > 0; i--) {
                int payload = starts + incs * i;
                format(builder, payload);
                if (i > 0) {
                    builder.append(", ");
                }
            }
            builder.append("]");
            System.out.println(builder.toString());
        }
        long start = System.currentTimeMillis();

        for (int index = 0; index < loops; ++index) {
            System.out.print(index % 10);

            test5(index, count, starts, incs, startM, incrementm, registry, slop);

            if (index % 10 == 0) {
                System.out.printf("\b" + index);
            }
            else if (index % 3 == 0) {
                System.out.printf("\b*");
            }
            else {
                System.out.printf("\b~");
            }
        }
        long end = System.currentTimeMillis();
        times.put(label, end - start);
    }

    //    @Test
    public void testPerfRepeated1kto10k() throws Exception
    {
        System.out.printf("\nTest Perfs 1k-10k Repeated %s times ", LOOPS_2);
        StringBuilder builder = new StringBuilder();
        builder.append(" on a matrix [");
        for (int powerMsg = POWER_2_MESSAGE + 1; powerMsg > 0; powerMsg--) {
            int message = 2 << powerMsg;
            format(builder, message);
            if (powerMsg > 0) {
                builder.append(", ");
            }
        }
        builder.append("] messages x size ");
        builder.append("[");
        for (int i = POWER_2_PAYLOAD + 1; i > 0; i--) {
            int payload = 1256 + 256 * i;
            format(builder, payload);
            if (i > 0) {
                builder.append(", ");
            }
        }
        builder.append("]");
        System.out.println(builder.toString());
        long start = System.currentTimeMillis();

        for (int index = 0; index < LOOPS_2; ++index) {
            System.out.print(index % 10);

            test3(index, 1256, 256, registry);

            if (index % 10 == 0) {
                System.out.print("\b" + index);
            }
            else if (index % 3 == 0) {
                System.out.print("\b*");
            }
            else {
                System.out.print("\b~");
            }
        }
        long end = System.currentTimeMillis();
        times.put("1000-10k", end - start);
    }

    private void format(StringBuilder builder, int payload)
    {
        builder.append(MetricReader.format(payload));
    }

    private void test(int index, int payld, int msg, MetricRegistry registry, int shift) throws Exception
    {
        int portNumber = 12345;

        int port = portNumber;
        for (int powerMsg = msg; powerMsg > 0; powerMsg--) {
            for (int powerPayload = payld; powerPayload > 0; powerPayload--) {
                if (powerPayload * shift > 5 && powerMsg * shift > 3) {
                    // skip really high values. you don't want this now
                    continue;
                }
                if (powerPayload * shift > 7 && powerMsg > 0) {
                    // skip really high values. you don't want this now
                    continue;
                }
                if (powerPayload * shift > 6 && powerMsg * shift > 2) {
                    // skip really high values. you don't want this now
                    continue;
                }
                if (powerMsg * shift > 4 && powerPayload * shift > 3) {
                    // skip really high values. you don't want this now
                    continue;
                }
                if (powerMsg * shift > 5 && powerPayload * shift > 2) {
                    // skip really high values. you don't want this now
                    continue;
                }
                ++port;
                boolean sign = false;
                String signed = WAIT;
                sign = powerPayload > 3 | powerMsg > 4 & powerMsg > 1;
                if (powerPayload < 1) {
                    sign = false;
                }
                if (!sign) {
                    signed = "";
                    //Helper.repeat(WAIT.length(), " ");
                }
                if (powerMsg == 3 && powerPayload == 2) {
                    signed = WARNING;
                }

                long message = (long) Math.pow(10, powerMsg);
                int payload = (int) Math.pow(10, powerPayload) * shift;

                System.out.printf(signed);
                test(port, message, payload, registry, MetricReader.getMetricsSeedName());
                System.out.print(Helper.erase(signed));
            }
        }
    }

    private void test2(int index, MetricRegistry registry) throws Exception
    {
        int portNumber = 12345;

        int port = portNumber;
        for (int powerMsg = POWER_2_MESSAGE + 1; powerMsg > 0; powerMsg--) {
            for (int powerPayload = POWER_2_PAYLOAD + 1; powerPayload > 0; powerPayload--) {
                if (powerPayload % 2 == 0) {
                    powerPayload--;
                }
                if (powerMsg % 2 == 0) {
                    powerPayload--;
                }
                if (powerPayload < 0) {
                    break;
                }
                if (powerPayload > 12 && powerMsg > 8) {
                    // skip really high values. you don't want this now
                    continue;
                }
                if (powerPayload > 6 && powerMsg > 9) {
                    // skip really high values. you don't want this now
                    continue;
                }
                if (powerMsg > 12 && powerPayload > 8) {
                    // skip really high values. you don't want this now
                    continue;
                }
                if (powerMsg > 16 && powerPayload > 9) {
                    // skip really high values. you don't want this now
                    continue;
                }
                ++port;
                boolean sign = false;
                String signed = WAIT;
                sign = powerPayload > 8 | powerMsg > 8 & powerMsg > 6;
                if (!sign) {
                    signed = "";
                }

                System.out.printf(signed);
                long message = 2 << powerMsg;
                int payload = 2 << powerPayload;

                test(port, message, payload, registry, MetricReader.getMetricsSeedName());
                System.out.print(Helper.erase(signed));
            }
        }
    }

    private void test3(int index, int start, int inc, MetricRegistry registry) throws Exception
    {
        int portNumber = 12345;

        int port = portNumber;

        for (int i = 10; i > 0; i--) {
            int payload = start + inc * i;
            for (int powerMsg = POWER_2_MESSAGE + 1; powerMsg > 0; powerMsg--) {
                int power = powerMsg;
                if (power % 2 == 0) {
                    power--;
                }
                if (i % 2 == 0) {
                    power--;
                }
                if (power < 0) {
                    break;
                }
                if (i < 0) {
                    break;
                }
                ++port;
                boolean sign = false;
                String signed = WAIT;

                if (!sign) {
                    signed = "";
                }

                long message = 2 << power;
                System.out.printf(signed);

                test(port, message, payload, registry, MetricReader.getMetricsSeedName());
                System.out.print(Helper.erase(signed));
            }
        }
        //        System.out.print("\b\b");
    }

    //    private void test4(int index, final int startS, final int incS, final int startM, final int incM,
    //                       MetricRegistry registry, boolean slop)
    //            throws Exception
    //    {
    //        test5(index, 10, startS, incS, startM, incM, registry, slop);
    //    }
    //
    private void test5(int index, int count, final int startS, final int incS, final int startM, final int incM,
                       MetricRegistry registry, boolean slop)
            throws Exception
    {
        int portNumber = 12345;

        int port = portNumber;

        for (int i = count; i > 0; i--) {
            int message = startM + incM * i;
            if (slop) {
                int power = 10 - i;
                int inc = incS;
                ++port;

                int payload = startS + (inc * power);

                test(port, message, payload, registry, MetricReader.getMetricsSeedName());
            }
            else {
                for (int powerPld = 10; powerPld > 0; powerPld--) {
                    int power = powerPld;
                    if (power % 2 == 0) {
                        power--;
                    }
                    if (i % 2 == 0) {
                        power--;
                    }
                    if (power < 0) {
                        continue;
                    }
                    ++port;
                    int inc = incS * power;

                    int payload = startS + inc;

                    test(port, message, payload, registry, MetricReader.getMetricsSeedName());
                }
            }
        }
        //        System.out.print("\b\b");
    }

    private int custom(int payload, int portNumber, long message, MetricRegistry registry) throws InterruptedException
    {
        int pld = 0;
        int iter = 10;
        switch (payload) {
        case 10:
            pld = 1;
            iter = 2;
            break;
        case 100:
            pld = 1;
            iter = 6;
            break;
        case 1000:
            pld = 1;
            iter = 2;
            break;
        case 10000:
            pld = 1;
            iter = 3;
            break;
        default:
            break;
        }
        final char[] cs = { '|', '/', '-', '\\' };
        if (pld != 0) {
            final int counter = 0;
            for (int idx = iter; idx < 10; idx += iter) {
                pld = payload * idx;
                //                System.out.printf("\b%s", cs[counter++]);
                test(++portNumber, message, pld, registry, MetricReader.getMetricsSeedName());
            }
        }

        switch (payload) {
        case 10:
            pld = 64;
            test(++portNumber, message, pld, registry, MetricReader.getMetricsSeedName());
            break;
        case 100:
            pld = 512;
            test(++portNumber, message, pld, registry, MetricReader.getMetricsSeedName());
            pld = 580;
            break;
        case 1000:
            pld = 8192;
            break;
        default:
            break;
        }
        if (pld != 0) {
            test(++portNumber, message, pld, registry, MetricReader.getMetricsSeedName());
        }
        return portNumber;
    }

    private void test(int portNumber, final long msgs, final int payload, final MetricRegistry registry,
                      final String seed)
            throws InterruptedException
    {
        final Integer port = new Integer(portNumber);
        final CyclicBarrier barrier = new CyclicBarrier(2, new Runnable()
        {
            @Override
            public void run()
            {
                // client and server are ready.
            }
        });
        final Exchanger<Long> exchanger = null; //new Exchanger<Long>();
        final CountDownLatch stopLatch = new CountDownLatch(2);
        Thread server = new Thread()
        {
            @Override
            public void run()
            {
                try {
                    String testName = MetricReader.getMetricsSeedName() + ".msgs." + msgs + ".size." + payload;
                    new PushConnect(null, testName, barrier, exchanger).run("tcp://localhost:" + port, payload, msgs);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                finally {
                    stopLatch.countDown();
                }
            }
        };
        Client client = new Client(port, payload, msgs, barrier, exchanger, stopLatch, registry);
        client.start();
        server.start();
        String formattedSize = MetricReader.format(payload);
        String formattedMsg = MetricReader.format(msgs);
        System.out.printf("=%sx%s", formattedMsg, formattedSize);

        stopLatch.await();
        System.out.printf(Helper.repeat(2 + 5 + formattedSize.length() + formattedMsg.length(), "\b \b"));
    }

    private class Client extends Thread
    {
        //      private final CountDownLatch startLatch = new CountDownLatch(1);

        private final String          port;
        private final long            size;
        private final long            messages;
        private final CountDownLatch  stopLatch;
        private final MetricRegistry  registry;
        private final CyclicBarrier   barrier;
        private final Exchanger<Long> exchanger;

        Client(int port, long size, long messages, CyclicBarrier barrier, Exchanger<Long> exchanger,
                CountDownLatch stopLatch, MetricRegistry registry)
        {
            this.barrier = barrier;
            this.exchanger = exchanger;
            this.port = Integer.toString(port);
            this.size = size;
            this.messages = messages;
            this.stopLatch = stopLatch;
            this.registry = registry;

        }

        @Override
        public void run()
        {
            try {
                String testName = MetricReader.getMetricsSeedName() + ".msgs." + messages + ".size." + size;
                double[] results = new PullBind(null, testName, barrier, exchanger)
                        .run("tcp://localhost:" + port, size, messages);

                double elapsed = results[0];
                int messageSize = (int) results[1];
                int messageCount = (int) results[2];
                int throughput = (int) results[3];
                int megabits = (int) results[4];
                int messagesReceived = (int) results[5];

                testName = MetricReader.getMetricsSeedName() + ".msgs." + messageCount + ".size." + messageSize;

                int drops = messageCount - messagesReceived;

                //              histogram = registry.histogram("Drops.h." + "." + testName);
                //              histogram.update(drops);
                if (drops != 0) {
                    Counter meter = registry.counter("Drops" + "." + testName);
                    meter.inc();
                    System.out.printf(
                                      " Lost %s Dropped messages of msg x size [%s x %s]\n",
                                      drops,
                                      MetricReader.format(messageCount),
                                      MetricReader.format(messageSize));
                }
                else {
                    Histogram histogram = null;

                    histogram = registry.histogram("Mb_s" + "." + testName);
                    histogram.update(megabits);

                    histogram = registry.histogram("Th" + "." + testName);
                    histogram.update(throughput);
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            finally {
                stopLatch.countDown();
            }
        }
    };
}
