package zmq.socket.pubsub.tree;

import org.junit.Before;
import org.junit.Test;
import zmq.Msg;
import zmq.ZMQ;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class RadixTreeTest
{
    private RadixTree tree;

    @Before
    public void setUp()
    {
        tree = new RadixTree();
    }

    @Test
    public void testEmpty()
    {
        assertThat(tree.size(), is(0));
    }

    @Test
    public void testAddSingleEntry()
    {
        assertThat(add("\1foo"), is(true));
    }

    @Test
    public void testAddSameEntryTwice()
    {
        add("\1test");
        assertThat(add("\1test"), is(false));
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
        add("0");
        assertThat(rm("1"), is(true));
    }

    @Test
    public void testCheckEmpty()
    {
        assertThat(check("foo"), is(false));
    }

    @Test
    public void testCheckAddedEntry()
    {
        add("1entry");
        assertThat(check("entry"), is(true));
    }

    @Test
    public void testCheckCommonPrefix()
    {
        add("2introduce");
        add("1introspect");
        assertThat(check("intro"), is(false));
    }

    @Test
    public void testCheckPrefix()
    {
        add("1toasted");
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
        add("1foo");
        assertThat(check("foobar"), is(true));
    }

    @Test
    public void testCheckNullEntryAdded()
    {
        add("0");
        assertThat(check("all queries return true"), is(true));
    }

    @Test
    public void testSize()
    {
        List<String> keys = Arrays.asList("1tester", "2water", "3slow", "4slower", "5test", "6team", "7toast");

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
            add(0x1 + key);
        }

        Set<String> result = new HashSet<>();
        tree.apply((data, size, pipe) -> result.add(new String(data, ZMQ.CHARSET)), null);
        assertThat(result, is(new HashSet<>(keys)));
    }

    @Test
    public void testApplyEmpty()
    {
        add("" + 0x1);

        Set<byte[]> result = new HashSet<>();
        tree.apply((data, size, pipe) -> result.add(data), null);
        assertThat(result.size(), is(1));
        assertThat(result.iterator().next().length, is(0));
    }

    @Test
    public void testApplyEmptyWithOthers()
    {
        add("" + 0x1);
        add("abcd");

        List<byte[]> result = new ArrayList<>();
        tree.apply((data, size, pipe) -> result.add(data), null);
        assertThat(result.size(), is(2));

        Iterator<byte[]> iterator = result.iterator();
        assertThat(iterator.next().length, is(0));

        byte[] last = iterator.next();
        assertThat(last.length, is(3));
        assertThat(last, is("bcd".getBytes(ZMQ.CHARSET)));
    }

    private boolean add(String key)
    {
        return tree.add(transform(key), 1, Integer.MIN_VALUE);
    }

    private boolean rm(String key)
    {
        return tree.rm(transform(key), 1, Integer.MIN_VALUE);
    }

    private boolean check(String key)
    {
        return tree.check(transform(key).buf());
    }

    private Msg transform(String msg)
    {
        return new Msg(msg.getBytes(ZMQ.CHARSET));
    }
}
