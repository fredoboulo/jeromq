package zmq.socket.pubsub.radix;

import zmq.util.function.BiConsumer;
import zmq.util.function.Function;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class Node
{
    static class Entry {
        final Byte key;
        final Node value;

        Entry(Byte key, Node value)
        {
            this.key = key;
            this.value = value;
        }
    }

    private static class RootNode extends Node {
        private RootNode()
        {
            super(0, 0);
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

    private final List<Byte> keys = new ArrayList<>();
    private final List<Node> values = new ArrayList<>();

    private int prefixLength;
    private ByteBuffer prefix;
    private int refCount;

    private Node(int refCount, int prefixLength)
    {
        this.refCount = refCount;
        this.prefixLength = prefixLength;
    }

    private Node(int refCount, int prefixLength, ByteBuffer prefix)
    {
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
        return values.size();
    }

    void edgeAt(int index, byte firstByte, Node node)
    {
        assert (index <= edgesCount());
        keys.add(index, firstByte);
        values.add(index, node);
    }

    Node split(int prefixBytesMatched) {
        ByteBuffer newKey = prefix.duplicate();
        newKey.position(prefixBytesMatched);

        Node splitNode = makeNode(refCount,
                prefixLength - prefixBytesMatched, newKey);
        splitNode.nodes(this);

        // Resize the current node to hold only the matched characters from its prefix.
        newKey = prefix.duplicate();
        newKey.limit(prefixBytesMatched);
        prefix(newKey);
        assert (prefixLength == prefixBytesMatched);
        keys.clear();
        values.clear();

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

    Iterable<Entry> entries() {
        List<Entry> entries = new ArrayList<>(keys.size());
        for (int idx = 0; idx < keys.size(); ++idx) {
            entries.add(new Entry(keys.get(idx), values.get(idx)));
        }
        return entries;
    }

    Iterable<Node> nodes() {
        return values;
    }

    Node firstNode() {
        assert (values.size() > 0);
        return values.get(0);
    }

    Node secondNode() {
        assert (values.size() > 1);
        return values.get(1);
    }

    void nodes(Node other)
    {
        keys.clear();
        values.clear();
        keys.addAll(other.keys);
        values.addAll(other.values);
    }

    <K, T> void visitKeys(List<ByteBuffer> buffer, BiConsumer<K,T> function, Function<List<ByteBuffer>,K> mapper, T arg)
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
