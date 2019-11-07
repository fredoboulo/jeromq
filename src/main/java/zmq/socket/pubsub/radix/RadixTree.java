package zmq.socket.pubsub.radix;

import zmq.ZMQ;
import zmq.util.function.BiConsumer;
import zmq.util.function.Function;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class RadixTree
{
    private static class MatchResult
    {
        private final int keyBytesMatched;
        private final int prefixBytesMatched;
        private final int edgeIndex;
        private final Node currentNode;
        private final Node parentNode;

        MatchResult(int keyBytesMatched, int prefixBytesMatched, int edgeIndex, Node currentNode, Node parentNode)
        {
            this.keyBytesMatched = keyBytesMatched;
            this.prefixBytesMatched = prefixBytesMatched;
            this.edgeIndex = edgeIndex;
            this.currentNode = currentNode;
            this.parentNode = parentNode;
        }
    }

    private final Node root;
    private int size;

    public RadixTree() {
        this(Node.makeRootNode());
    }

    RadixTree(Node root) {
        this.root = root;
    }

    /**
     * Add key to the tree.
     * @param key the key to add.
     * @return true if this was a new key, false if it is a duplicate.
     */
    public boolean add(String key) {
        return op(key, this::add);
    }

    /**
     * Remove key from the tree.
     * @param key the key to remove.
     * @return true if the item is actually removed from the tree.
     */
    public boolean rm(String key) {
        return op(key, this::rm);
    }

    /**
     * Check whether particular key is in the tree.
     * @param key the key to check.
     * @return true if the key is in the tree, otherwise false.
     */
    public boolean check(String key) {
        return op(key, this::check);
    }

    public <T> void apply(BiConsumer<String, T> function, T arg) {
        apply(function, this::transform, arg);
    }

    public int size() {
        return root.size();
    }

    private MatchResult match(ByteBuffer key, boolean isLookup) {
        assert (key != null) : "Key cannot be null when looking for matches";
        // Node we're currently at in the traversal and its predecessors.
        Node currentNode = root;
        Node parentNode = currentNode;

        int keyByteIndex = 0; // Index of the next byte to match in the key.
        int prefixByteIndex = 0; // Index of the next byte to match in the current node's prefix.
        int edgeIndex = 0; // Index of the edge from parent to current node.

        int keySize = key.remaining();
        while(currentNode.prefixLength() > 0 || currentNode.edgesCount() > 0) {
            for (prefixByteIndex = 0;
                 prefixByteIndex < currentNode.prefixLength() && keyByteIndex < keySize;
                 ++prefixByteIndex, ++keyByteIndex) {
                if( currentNode.prefix(prefixByteIndex) != key.get(keyByteIndex)) {
                    break;
                }
            }
            // Even if a prefix of the key matches and we're doing a
            // lookup, this means we've found a matching subscription.
            if (isLookup && prefixByteIndex == currentNode.prefixLength() && currentNode.refCount() > 0) {
                keyByteIndex  = keySize;
                break;
            }

            // There was a mismatch or we've matched the whole key, so
            // there's nothing more to do.
            if (prefixByteIndex != currentNode.prefixLength() || keyByteIndex == keySize) {
                break;
            }

            // We need to match the rest of the key. Check if there's an
            // outgoing edge from this node.
            Node nextNode = currentNode;
            for (Node.Entry entry : currentNode.entries()) {
                if (entry.key.equals(key.get(keyByteIndex))) {
                    edgeIndex = 1;
                    nextNode = entry.value;
                    break;
                }
            }

            if (nextNode == currentNode) {
                break; // no outgoing edge
            }
            parentNode = currentNode;
            currentNode = nextNode;
        }

        return new MatchResult(keyByteIndex, prefixByteIndex, edgeIndex, currentNode, parentNode);
    }

    private boolean op(String key, Function<ByteBuffer, Boolean> func) {
        byte[] bytes = key.getBytes(ZMQ.CHARSET);
        return func.apply(ByteBuffer.wrap(bytes));
    }

    boolean add(ByteBuffer key) {
        MatchResult result = match(key, false);
        int keyBytesMatched = result.keyBytesMatched;
        int prefixBytesMatched = result.prefixBytesMatched;
        Node currentNode = result.currentNode;

        int keySize = key.remaining();
        if (keyBytesMatched != keySize) {
            // Not all characters match, we might have to split the node.
            if (prefixBytesMatched == currentNode.prefixLength()) {
                // The mismatch is at one of the outgoing edges, so we
                // create an edge from the current node to a new leaf node
                // that has the rest of the key as the prefix.
                ByteBuffer newKey = key.duplicate();
                newKey.position(keyBytesMatched);
                Node keyNode = Node.makeNode(1, keySize - keyBytesMatched, newKey);

                currentNode.edgeAt(currentNode.edgesCount(), key.get(keyBytesMatched), keyNode);
                ++size;
                return true;
            }
            // There was a mismatch, so we need to split this node.

            // Create two nodes that will be reachable from the parent.
            // One node will have the rest of the characters from the key,
            ByteBuffer newKey = key.duplicate();
            newKey.position(keyBytesMatched);
            Node keyNode = Node.makeNode(1, keySize - keyBytesMatched, newKey);
            // and the other node will have the rest of the characters
            // from the current node's prefix.
            Node splitNode = currentNode.split(prefixBytesMatched);
            currentNode.refCount(0);

            // Add links to the new nodes.
            currentNode.edgeAt(0, keyNode.prefix(0), keyNode);
            currentNode.edgeAt(1, splitNode.prefix(0), splitNode);
            ++size;
            return true;
        }

        // All characters in the key match, but we still might need to split.
        if (prefixBytesMatched != currentNode.prefixLength()) {
            // All characters in the key match, but not all characters
            // from the current node's prefix match.

            // Create a node that contains the rest of the characters from
            // the current node's prefix and the outgoing edges from the current node.
            Node splitNode = currentNode.split(prefixBytesMatched);

            // Add the split node as an edge and set the refcount to 1
            // since this key wasn't inserted earlier.
            currentNode.edgeAt(0, splitNode.prefix(0), splitNode);
            currentNode.refCount(1);

            ++size;
            return true;
        }

        ++size;
        currentNode.refCount(currentNode.refCount() + 1);

        return currentNode.refCount() == 1;
    }

    boolean rm(ByteBuffer key) {
        MatchResult result = match(key, false);
        int keyBytesMatched = result.keyBytesMatched;
        int prefixBytesMatched = result.prefixBytesMatched;
        int edgeIndex = result.edgeIndex;
        Node currentNode = result.currentNode;
        Node parentNode = result.parentNode;

        int keySize = key.remaining();
        if (keyBytesMatched != keySize || prefixBytesMatched != currentNode.prefixLength() || currentNode.refCount() == 0) {
            return false;
        }
        currentNode.refCount(currentNode.refCount() -1);
        --size;
        if (currentNode.refCount() > 0) {
            return false;
        }
        // Don't delete the root node.
        if (currentNode == root) {
            return true;
        }
        int outgoingEdges = currentNode.edgesCount();
        if (outgoingEdges > 1) {
            // This node can't be merged with any other node, so there's
            // nothing more to do.
            return true;
        }
        if (outgoingEdges == 1) {
            // Merge this node with the single child node.
            Node child = currentNode.firstNode();
            // Append the child node's prefix to the current node.
            currentNode.prefix(concat(currentNode.prefix(), child.prefix()));
            // Copy the rest of child node's data to the current node.
            currentNode.nodes(child);
            currentNode.refCount(child.refCount());
            return true;
        }

        if  (parentNode.edgesCount() == 2 && parentNode.refCount() == 0 && parentNode != root) {
            // Removing this node leaves the parent with one child.
            // If the parent doesn't hold a key or if it isn't the root,
            // we can merge it with its single child node.
            assert (edgeIndex < 2);
            Node otherChild = parentNode.firstNode();
            if (edgeIndex == 0) {
                otherChild = parentNode.secondNode();
            }
            // Append the child node's prefix to the parent node.
            parentNode.prefix(concat(currentNode.prefix(), otherChild.prefix()));
            // Copy the rest of child node's data to the parent node.
            parentNode.nodes(otherChild);
            parentNode.refCount(otherChild.refCount());

            return true;
        }
        // This is a leaf node that doesn't leave its parent with one
        // outgoing edge. Remove the outgoing edge to this node from the
        // parent.
        assert (outgoingEdges == 0);

        // Replace the edge to the current node with the last edge. An
        // edge consists of a byte and a pointer to the next node. First
        // replace the byte.
        int lastIndex = parentNode.edgesCount() - 1;
        // byte lastByte = parentNode.prefix(lastIndex);

        // Move the chunk of pointers one byte to the left, effectively
        // deleting the last byte in the region of first bytes by
        // overwriting it.

        // Shrink the parent node to the new size, which "deletes" the
        // last pointer in the chunk of node pointers.

        // Nothing points to this node now, so we can reclaim it.

        return true;
    }

    private ByteBuffer concat(ByteBuffer first, ByteBuffer second) {
        ByteBuffer result = ByteBuffer.allocate(first.remaining() + second.remaining());
        result.put(first);
        result.put(second);
        return result;
    }

    boolean check(ByteBuffer key) {
        if (root.refCount() > 0) {
            return true;
        }
        MatchResult result = match(key, true);
        int keySize = key.remaining();
        return result.keyBytesMatched == keySize
                && result.prefixBytesMatched == result.currentNode.prefixLength()
                && result.currentNode.refCount() > 0;
    }

    private String transform(List<ByteBuffer> bytes) {
        return new String(toBytes(bytes), ZMQ.CHARSET);
    }

    byte[] toBytes(List<ByteBuffer> bytes) {
        try(ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            for (ByteBuffer buf : bytes) {
                buf = buf.duplicate();
                while (buf.hasRemaining()) {
                    stream.write(buf.get());
                }
            }
            return stream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Apply the function supplied to each key in the tree.
     */
    <K, T> void apply(BiConsumer<K, T> function, Function<List<ByteBuffer>, K> mapper, T arg) {
        root.visitKeys(new LinkedList<>(), function, mapper, arg);
    }
}
