package zmq.socket.pubsub.radix;

import zmq.util.function.BiConsumer;
import zmq.util.function.Function;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class Node
{
    static class Entry
    {
        final Byte key;
        final Node value;

        Entry(Byte key, Node value)
        {
            this.key = key;
            this.value = value;
        }

        Entry(Map.Entry<Byte, Node> entry)
        {
            this(entry.getKey(), entry.getValue());
        }
    }

    private static class RootNode extends Node
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
            for (Node child : nodes()) {
                child.visitKeys(buffer, function, mapper, arg);
            }
        }

        @Override
        public String toString()
        {
            return "RootNode";
        }
    }

    private final LinkedHashMap<Byte, Node> nodes = new LinkedHashMap<>();

    private int prefixLength;
    private ByteBuffer prefix;
    private int refCount;

    private Node(int refCount, int prefixLength, ByteBuffer prefix)
    {
        assert (prefix != null) : "Prefix of a node in a radix tree has to be non null";
        this.refCount = refCount;
        prefix(prefix);
        assert (this.prefixLength == prefixLength);
    }

    static Node makeNode(int refCount, int prefixLength, ByteBuffer prefix)
    {
        return new Node(refCount, prefixLength, prefix);
    }

    static Node makeRootNode()
    {
        return new RootNode();
    }

    int size()
    {
        int size = refCount;
        for (Node child : this.nodes()) {
            size += child.size();
        }
        return size;
    }

    int edgesCount()
    {
        return nodes.size();
    }

    void edgeAt(int index, byte firstByte, Node node)
    {
        assert (index <= edgesCount());
        nodes.put(firstByte, node);
    }

    public void rm(int index, byte firstByte, Node node)
    {
        assert (index <= edgesCount());
        assert (nodes.get(firstByte) == node);
        boolean rc = nodes.remove(firstByte, node);
        assert (rc) : "Node was not removed";
    }

    Node split(int prefixBytesMatched)
    {
        ByteBuffer newKey = prefix.duplicate();
        newKey.position(prefixBytesMatched + prefix.position());

        Node splitNode = makeNode(refCount,
                prefixLength - prefixBytesMatched, newKey);
        splitNode.nodes(this);

        // Resize the current node to hold only the matched characters from its prefix.
        newKey = prefix.duplicate();
        newKey.limit(prefixBytesMatched + prefix.position());
        prefix(newKey);
        assert (prefixLength == prefixBytesMatched);
        nodes.clear();

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
        List<Entry> entries = new ArrayList<>(nodes.size());
        for (Map.Entry<Byte, Node> entry : nodes.entrySet()) {
            entries.add(new Entry(entry));
        }
        return entries;
    }

    Iterable<Node> nodes()
    {
        return nodes.values();
    }

    Node firstNode()
    {
        return nthNode(0);
    }

    Node secondNode()
    {
        return nthNode(1);
    }

    private Node nthNode(int index)
    {
        assert (nodes.size() > index);
        int count = 0;
        for (Map.Entry<Byte, Node> entry : nodes.entrySet()) {
            ++count;
            if (count > index) {
                return entry.getValue();
            }
        }
        throw new IllegalStateException("No node at index " + index);
    }

    void nodes(Node other)
    {
        nodes.clear();
        nodes.putAll(other.nodes);
    }

    <K, T> void visitKeys(List<ByteBuffer> buffer, BiConsumer<K, T> function, Function<List<ByteBuffer>, K> mapper, T arg)
    {
        buffer.add(prefix);
        if (refCount > 0) {
            function.accept(mapper.apply(buffer), arg);
        }
        for (Node child : nodes()) {
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
}
