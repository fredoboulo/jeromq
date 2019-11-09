package zmq.socket.pubsub.tree;

import org.junit.Before;
import org.junit.Test;
import zmq.ZMQ;
import zmq.util.Clock;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

public class PerformanceRadixTreeTest
{
    private final int nkeys = 10_000;
    private final int nQueries = 1_000_000;
    private final int warmupRuns = 100;
    private final int samples = 50;
    private final int keyLength = 20;

    private final byte[] chars = "abcdefghijklmnopqrstuvwxyz0123456789".getBytes(ZMQ.CHARSET);

    private List<ByteBuffer> queries;

    @Before
    public void setUp()
    {
        queries = new ArrayList<>(nQueries);
        List<byte[]> keys = new ArrayList<>(nkeys);
        int key = nkeys;
        Random rnd = new Random();
        while (key-- > 0) {
            byte[] bytes = new byte[rnd.nextInt(keyLength) + 2];
            for (int idx = 0; idx < bytes.length; ++idx) {
                bytes[idx] = chars[rnd.nextInt(chars.length)];
            }
            keys.add(bytes);
        }
        int que = nQueries;
        while (que-- > 0) {
            queries.add(ByteBuffer.wrap(keys.get(rnd.nextInt(keys.size()))));
        }
    }

    @Test
    public void testBenchmarkLookup()
    {
        assertBenchmarkLookup(new RadixTree(), queries);
        assertBenchmarkLookup(new Trie(), queries);
    }

    private void assertBenchmarkLookup(Tree tree, Collection<ByteBuffer> queries)
    {
        int run = warmupRuns;
        while (run-- > 0) {
            for (ByteBuffer query : queries) {
                tree.check(query);
            }
        }
        run = samples;
        List<Long> durations = new ArrayList<>(run);
        while (run-- > 0) {
            long interval = 0;
            for (ByteBuffer query : queries) {
                long start = Clock.nowNS();
                tree.check(query);
                long end = Clock.nowNS();
                interval += end - start;
            }
            durations.add(interval);
        }

        double sum = durations.stream().mapToDouble(l -> l / (double) queries.size()).sum();
        System.out.println("Average lookup for " + tree.getClass().getSimpleName() + ": " + sum / durations.size());
    }
}
