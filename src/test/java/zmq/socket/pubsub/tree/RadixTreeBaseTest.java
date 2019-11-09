package zmq.socket.pubsub.tree;

import org.junit.Before;
import org.junit.Test;
import zmq.ZMQ;
import zmq.util.Utils;
import zmq.util.function.BiConsumer;
import zmq.util.function.Function;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class RadixTreeBaseTest
{
    private RadixTreeBase tree;

    @Before
    public void setUp()
    {
        tree = new RadixTreeBase();
    }

    @Test
    public void testEmpty()
    {
        assertThat(tree.size(), is(0));
    }

    @Test
    public void testAddSingleEntry()
    {
        assertThat(add("foo"), is(true));
    }

    @Test
    public void testAddSameEntryTwice()
    {
        add("test");
        assertThat(add("test"), is(false));
    }

    @Test
    public void testRemoveWhenEmpty()
    {
        assertThat(rm("test"), is(false));
    }

    @Test
    public void testRemoveSingleEntry()
    {
        add("test");
        assertThat(rm("test"), is(true));
    }

    @Test
    public void testRemoveSingleEntryTwice()
    {
        add("test");
        rm("test");
        assertThat(rm("test"), is(false));
    }

    @Test
    public void testRemoveDuplicateEntry()
    {
        add("test");
        add("test");
        assertThat(rm("test"), is(false));
        assertThat(rm("test"), is(true));
    }

    @Test
    public void testRemoveCommonPrefix()
    {
        add("checkpoint");
        add("checklist");
        assertThat(rm("check"), is(false));
    }

    @Test
    public void testRemoveCommonPrefixEntry()
    {
        add("checkpoint");
        add("checklist");
        add("check");
        assertThat(rm("check"), is(true));
    }

    @Test
    public void testRemoveNullEntry()
    {
        add("");
        assertThat(rm(""), is(true));
    }

    @Test
    public void testCheckEmpty()
    {
        assertThat(check("foo"), is(false));
    }

    @Test
    public void testCheckAddedEntry()
    {
        add("entry");
        assertThat(check("entry"), is(true));
    }

    @Test
    public void testCheckCommonPrefix()
    {
        add("introduce");
        add("introspect");
        assertThat(check("intro"), is(false));
    }

    @Test
    public void testCheckPrefix()
    {
        add("toasted");
        assertThat(check("toast"), is(false));
        assertThat(check("toaste"), is(false));
        assertThat(check("toaster"), is(false));
    }

    @Test
    public void testCheckNonExistentEntry()
    {
        add("red");
        assertThat(check("blue"), is(false));
    }

    @Test
    public void testCheckQueryLongerThanEntry()
    {
        add("foo");
        assertThat(check("foobar"), is(true));
    }

    @Test
    public void testCheckNullEntryAdded()
    {
        add("");
        assertThat(check("all queries return true"), is(true));
    }

    @Test
    public void testSize()
    {
        List<String> keys = Arrays.asList("tester", "water", "slow", "slower", "test", "team", "toast");

        for (String key : keys) {
            assertThat(key, add(key), is(true));
        }
        assertThat(tree.size(), is(keys.size()));

        for (String key : keys) {
            assertThat(key, add(key), is(false));
        }
        assertThat(tree.size(), is(keys.size() * 2));

        for (String key : keys) {
            assertThat(key, rm(key), is(false));
        }
        assertThat(tree.size(), is(keys.size()));

        for (String key : keys) {
            assertThat(key, rm(key), is(true));
        }
        assertThat(tree.size(), is(0));
    }

    @Test
    public void testApply()
    {
        Collection<String> keys = Arrays.asList("tester", "water", "slow", "slower", "test", "team", "toast");
        for (String key : keys) {
            add(key);
        }

        Set<String> result = new HashSet<>();
        apply((key, list) -> list.add(key), result);
        assertThat(result, is(new HashSet<>(keys)));
    }

    public boolean add(String key)
    {
        return op(key, tree::add);
    }

    public boolean rm(String key)
    {
        return op(key, tree::rm);
    }

    public boolean check(String key)
    {
        return op(key, tree::check);
    }

    public <T> void apply(BiConsumer<String, T> function, T arg)
    {
        tree.apply(function, this::transform, arg);
    }

    private boolean op(String key, Function<ByteBuffer, Boolean> func)
    {
        byte[] bytes = key.getBytes(ZMQ.CHARSET);
        return func.apply(ByteBuffer.wrap(bytes));
    }

    private String transform(List<ByteBuffer> bytes)
    {
        return new String(Utils.toBytes(bytes), ZMQ.CHARSET);
    }
}
