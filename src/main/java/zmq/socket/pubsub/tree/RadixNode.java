package zmq.socket.pubsub.tree;

import zmq.util.function.BiConsumer;
import zmq.util.function.Function;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class RadixNode
{
    // the child nodes mapped by the first byte of their own prefix.
    private Map<Byte, RadixNode> nodes;

    // The number of characters in the node's prefix. The prefix is a
    // part of one or more keys in the tree, e.g. the prefix of each node
    // in a trie consists of a single character.
    private int prefixLength;

    // The node's prefix as a sequence of one or more bytes. The root
    // node always has an empty prefix, unlike other nodes in the tree.
    private ByteBuffer prefix;

    // The reference count of the key held by the node. This is 0 if
    // the node doesn't hold a key.
    private int refCount;
    private Iterable<Entry> entries;

    private RadixNode(int refCount, int prefixLength, ByteBuffer prefix)
    {
        assert (prefix != null) : "Prefix of a node in a radix tree has to be non null";
        this.refCount = refCount;
        prefix(prefix);
        assert (this.prefixLength == prefixLength);
    }

    static RadixNode makeNode(int refCount, int prefixLength, ByteBuffer prefix)
    {
        return new RadixNode(refCount, prefixLength, prefix);
    }

    static RadixNode makeRootNode()
    {
        return new RootNode();
    }

    int size()
    {
        int size = refCount;
        for (RadixNode child : this.nodes()) {
            size += child.size();
        }
        return size;
    }

    int edgesCount()
    {
        return nodes == null ? 0 : nodes.size();
    }

    void add(int index, byte firstByte, RadixNode node)
    {
        assert (index <= edgesCount());
        nodes = createNodesIfNeeded();
        nodes.put(firstByte, node);
        entries = computeEntries();
    }

    void remove(int index, byte firstByte, RadixNode node)
    {
        assert (index <= edgesCount());
        assert (nodes != null);
        assert (nodes.get(firstByte) == node);

        RadixNode old = nodes.remove(firstByte);

        assert (old == node) : "Node was not removed, but a different node was removed " + old;
        entries = computeEntries();
    }

    RadixNode split(int prefixBytesMatched)
    {
        ByteBuffer newKey = prefix.duplicate();
        newKey.position(prefixBytesMatched + prefix.position());

        RadixNode splitNode = makeNode(refCount,
                prefixLength - prefixBytesMatched, newKey);
        splitNode.nodes(this);

        // Resize the current node to hold only the matched characters from its prefix.
        newKey = prefix.duplicate();
        newKey.limit(prefixBytesMatched + prefix.position());
        prefix(newKey);
        assert (prefixLength == prefixBytesMatched);
        nodes = null;
        entries = null;

        return splitNode;
    }

    int prefixLength()
    {
        return prefixLength;
    }

    byte prefix(int index)
    {
        return prefix.get(prefix.position() + index);
    }

    void prefix(ByteBuffer prefix)
    {
        this.prefix = prefix;
        this.prefixLength = prefix.remaining();
    }

    ByteBuffer prefix()
    {
        return this.prefix;
    }

    int refCount()
    {
        return refCount;
    }

    void refCount(int count)
    {
        this.refCount = count;
    }

    Iterable<Entry> entries()
    {
        if (nodes == null) {
            return Collections.emptyList();
        }
        assert (entries != null);
        return entries;
    }

    private Iterable<Entry> computeEntries()
    {
        List<Entry> entries = new ArrayList<>(nodes.size());
        for (Map.Entry<Byte, RadixNode> entry : nodes.entrySet()) {
            entries.add(new Entry(entry));
        }
        return entries;
    }

    Iterable<RadixNode> nodes()
    {
        if (nodes == null) {
            return Collections.emptyList();
        }
        return nodes.values();
    }

    RadixNode firstNode()
    {
        return nthNode(0);
    }

    RadixNode secondNode()
    {
        return nthNode(1);
    }

    private RadixNode nthNode(int index)
    {
        assert (nodes != null);
        assert (nodes.size() > index);
        int count = 0;
        for (Map.Entry<Byte, RadixNode> entry : nodes.entrySet()) {
            ++count;
            if (count > index) {
                return entry.getValue();
            }
        }
        throw new IllegalStateException("No node at index " + index);
    }

    void nodes(RadixNode other)
    {
        if (other.nodes == null) {
            nodes = null;
            entries = null;
        }
        else {
            nodes = createNodesIfNeeded();
            nodes.clear();

            nodes.putAll(other.nodes);
            entries = computeEntries();
        }
    }

    private Map<Byte, RadixNode> createNodesIfNeeded()
    {
        if (nodes == null) {
            nodes = new LinkedHashMap<>();
            entries = Collections.emptyList();
        }
        return nodes;
    }

    <K, T> void visitKeys(List<ByteBuffer> buffer, BiConsumer<K, T> function, Function<List<ByteBuffer>, K> mapper, T arg)
    {
        buffer.add(prefix);
        if (refCount > 0) {
            function.accept(mapper.apply(buffer), arg);
        }
        for (RadixNode child : nodes()) {
            child.visitKeys(buffer, function, mapper, arg);
        }
        buffer.remove(buffer.size() - 1);
    }

    @Override
    public String toString()
    {
        return "Node{" +
                "refCount=" + refCount +
                ", prefix=" + toString(prefix) +
                '}';
    }

    private String toString(ByteBuffer prefix)
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int idx = prefix.position(); idx < prefix.limit(); ++idx) {
            out.write(prefix.get(idx));
        }
        return out.toString();
    }

    static final class Entry
    {
        final Byte key;
        final RadixNode value;

        Entry(Byte key, RadixNode value)
        {
            this.key = key;
            this.value = value;
        }

        Entry(Map.Entry<Byte, RadixNode> entry)
        {
            this(entry.getKey(), entry.getValue());
        }
    }

    private static final class RootNode extends RadixNode
    {
        private RootNode()
        {
            super(0, 0, ByteBuffer.allocate(0));
        }

        @Override
        <K, T> void visitKeys(List<ByteBuffer> buffer, BiConsumer<K, T> function, Function<List<ByteBuffer>, K> mapper, T arg)
        {
            if (refCount() > 0) {
                function.accept(mapper.apply(Collections.singletonList(prefix())), arg);
            }
            for (RadixNode child : nodes()) {
                child.visitKeys(buffer, function, mapper, arg);
            }
        }

        @Override
        public String toString()
        {
            return "RootNode";
        }
    }
}
