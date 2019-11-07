package zmq.socket.pubsub.radix;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class RadixTreeTest
{
    private RadixTree tree;

    @Before
    public void setUp() {
        tree = new RadixTree();
    }

    @Test
    public void testEmpty() {
        assertThat(tree.size(), is(0));
    }

    @Test
    public void testAddSingleEntry() {
        assertThat(tree.add("foo"), is(true));
    }

    @Test
    public void testAddSameEntryTwice() {
        tree.add("test");
        assertThat(tree.add("test"), is(false));
    }

    @Test
    public void testRemoveWhenEmpty() {
        assertThat(tree.rm("test"), is(false));
    }

    @Test
    public void testRemoveSingleEntry() {
        tree.add("test");
        assertThat(tree.rm("test"), is(true));
    }

    @Test
    public void testRemoveSingleEntryTwice() {
        tree.add("test");
        tree.rm("test");
        assertThat(tree.rm("test"), is(false));
    }

    @Test
    public void testRemoveDuplicateEntry() {
        tree.add("test");
        tree.add("test");
        assertThat(tree.rm("test"), is(false));
        assertThat(tree.rm("test"), is(true));
    }

    @Test
    public void testRemoveCommonPrefix() {
        tree.add("checkpoint");
        tree.add("checklist");
        assertThat(tree.rm("check"), is(false));
    }

    @Test
    public void testRemoveCommonPrefixEntry() {
        tree.add("checkpoint");
        tree.add("checklist");
        tree.add("check");
        assertThat(tree.rm("check"), is(true));
    }

    @Test
    public void testRemoveNullEntry() {
        tree.add("");
        assertThat(tree.rm(""), is(true));
    }

    @Test
    public void testCheckEmpty() {
        assertThat(tree.check("foo"), is(false));
    }

    @Test
    public void testCheckAddedEntry() {
        tree.add("entry");
        assertThat(tree.check("entry"), is(true));
    }

    @Test
    public void testCheckCommonPrefix() {
        tree.add("introduce");
        tree.add("introspect");
        assertThat(tree.check("intro"), is(false));
    }

    @Test
    public void testCheckPrefix() {
        tree.add("toasted");
        assertThat(tree.check("toast"), is(false));
        assertThat(tree.check("toaste"), is(false));
        assertThat(tree.check("toaster"), is(false));
    }

    @Test
    public void testCheckNonExistentEntry() {
        tree.add("red");
        assertThat(tree.check("blue"), is(false));
    }

    @Test
    public void testCheckQueryLongerThanEntry() {
        tree.add("foo");
        assertThat(tree.check("foobar"), is(true));
    }

    @Test
    public void testCheckNullEntryAdded() {
        tree.add("");
        assertThat(tree.check("all queries return true"), is(true));
    }

    @Test
    public void testSize() {
        List<String> keys = Arrays.asList("tester", "water", "slow", "slower", "test", "team", "toast");

        keys.forEach(key -> assertThat(key, tree.add(key), is(true)));
        assertThat(tree.size(), is(keys.size()));

        keys.forEach(key -> assertThat(key, tree.add(key), is(false)));
        assertThat(tree.size(), is(keys.size() * 2));

        keys.forEach(key -> assertThat(key, tree.rm(key), is(false)));
        assertThat(tree.size(), is(keys.size()));

        keys.forEach(key -> assertThat(key, tree.rm(key), is(true)));
        assertThat(tree.size(), is(0));
    }

    @Test
    public void testApply()
    {
        List<String> keys = Arrays.asList("tester", "water", "slow", "slower", "test", "team", "toast");
        keys.forEach(tree::add);

        List<String> result = new ArrayList<>();
        tree.apply((key, list) -> list.add(key), result);
        assertThat(new HashSet<>(result), is(new HashSet<>(keys)));
    }
}