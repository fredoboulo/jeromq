package zmq.socket.pubsub.tree;

import zmq.util.function.BiConsumer;
import zmq.util.function.Function;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

class RadixTreeBase
{
    private static class MatchResult
    {
        private final int keyBytesMatched;
        private final int prefixBytesMatched;
        private final int edgeIndex;
        private final RadixNode currentNode;
        private final RadixNode parentNode;

        MatchResult(int keyBytesMatched, int prefixBytesMatched, int edgeIndex, RadixNode currentNode, RadixNode parentNode)
        {
            this.keyBytesMatched = keyBytesMatched;
            this.prefixBytesMatched = prefixBytesMatched;
            this.edgeIndex = edgeIndex;
            this.currentNode = currentNode;
            this.parentNode = parentNode;
        }
    }

    private final RadixNode root;

    RadixTreeBase()
    {
        this.root = RadixNode.makeRootNode();
    }

    int size()
    {
        return root.size();
    }

    private MatchResult match(ByteBuffer key, boolean isLookup)
    {
        assert (key != null) : "Key cannot be null when looking for matches";
        // Node we're currently at in the traversal and its predecessors.
        RadixNode currentNode = root;
        RadixNode parentNode = currentNode;

        int keyByteIndex = 0; // Index of the next byte to match in the key.
        int prefixByteIndex = 0; // Index of the next byte to match in the current node's prefix.
        int edgeIndex = 0; // Index of the edge from parent to current node.

        int keySize = key.remaining();
        while (currentNode.prefixLength() > 0 || currentNode.edgesCount() > 0) {
            for (prefixByteIndex = 0;
                 prefixByteIndex < currentNode.prefixLength() && keyByteIndex < keySize;
                 ++prefixByteIndex, ++keyByteIndex) {
                if (currentNode.prefix(prefixByteIndex) != key.get(keyByteIndex + key.position())) {
                    break;
                }
            }
            // Even if a prefix of the key matches and we're doing a
            // lookup, this means we've found a matching subscription.
            if (isLookup && prefixByteIndex == currentNode.prefixLength() && currentNode.refCount() > 0) {
                keyByteIndex = keySize;
                break;
            }

            // There was a mismatch or we've matched the whole key, so
            // there's nothing more to do.
            if (prefixByteIndex != currentNode.prefixLength() || keyByteIndex == keySize) {
                break;
            }

            // We need to match the rest of the key. Check if there's an
            // outgoing edge from this node.
            RadixNode nextNode = currentNode;
            for (RadixNode.Entry entry : currentNode.entries()) {
                if (entry.key.equals(key.get(keyByteIndex + key.position()))) {
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

    /**
     * Add key to the tree.
     *
     * @param key the key to add.
     * @return true if this was a new key, false if it is a duplicate.
     */
    boolean add(ByteBuffer key)
    {
        assert (key != null) : "A key cannot be null in a radix tree";
        MatchResult result = match(key, false);
        int keyBytesMatched = result.keyBytesMatched;
        int prefixBytesMatched = result.prefixBytesMatched;
        RadixNode currentNode = result.currentNode;

        int keySize = key.remaining();
        if (keyBytesMatched != keySize) {
            // Not all characters match, we might have to split the node.
            if (prefixBytesMatched == currentNode.prefixLength()) {
                // The mismatch is at one of the outgoing edges, so we
                // create an edge from the current node to a new leaf node
                // that has the rest of the key as the prefix.
                ByteBuffer newKey = key.duplicate();
                newKey.position(keyBytesMatched + key.position());
                RadixNode keyNode = RadixNode.makeNode(1, keySize - keyBytesMatched, newKey);

                currentNode.add(currentNode.edgesCount(), key.get(keyBytesMatched + key.position()), keyNode);
                return true;
            }
            // There was a mismatch, so we need to split this node.

            // Create two nodes that will be reachable from the parent.
            // One node will have the rest of the characters from the key,
            ByteBuffer newKey = key.duplicate();
            newKey.position(keyBytesMatched + key.position());
            RadixNode keyNode = RadixNode.makeNode(1, keySize - keyBytesMatched, newKey);
            // and the other node will have the rest of the characters
            // from the current node's prefix.
            RadixNode splitNode = currentNode.split(prefixBytesMatched);
            currentNode.refCount(0);

            // Add links to the new nodes.
            currentNode.add(0, keyNode.prefix(0), keyNode);
            currentNode.add(1, splitNode.prefix(0), splitNode);
            return true;
        }

        // All characters in the key match, but we still might need to split.
        if (prefixBytesMatched != currentNode.prefixLength()) {
            // All characters in the key match, but not all characters
            // from the current node's prefix match.

            // Create a node that contains the rest of the characters from
            // the current node's prefix and the outgoing edges from the current node.
            RadixNode splitNode = currentNode.split(prefixBytesMatched);

            // Add the split node as an edge and set the refcount to 1
            // since this key wasn't inserted earlier.
            currentNode.add(0, splitNode.prefix(key.position()), splitNode);
            currentNode.refCount(1);

            return true;
        }

        currentNode.refCount(currentNode.refCount() + 1);

        return currentNode.refCount() == 1;
    }

    /**
     * Remove key from the tree.
     *
     * @param key the key to remove.
     * @return true if the item is actually removed from the tree.
     */
    boolean rm(ByteBuffer key)
    {
        assert (key != null) : "A key cannot be null in a radix tree";
        MatchResult result = match(key, false);
        int keyBytesMatched = result.keyBytesMatched;
        int prefixBytesMatched = result.prefixBytesMatched;
        int edgeIndex = result.edgeIndex;
        RadixNode currentNode = result.currentNode;
        RadixNode parentNode = result.parentNode;

        int keySize = key.remaining();
        if (keyBytesMatched != keySize || prefixBytesMatched != currentNode.prefixLength() || currentNode.refCount() == 0) {
            return false;
        }
        currentNode.refCount(currentNode.refCount() - 1);
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
            RadixNode child = currentNode.firstNode();
            // Append the child node's prefix to the current node.
            currentNode.prefix(expand(currentNode.prefix(), child.prefix()));
            // Copy the rest of child node's data to the current node.
            currentNode.nodes(child);
            currentNode.refCount(child.refCount());
            return true;
        }

        if (parentNode.edgesCount() == 2 && parentNode.refCount() == 0 && parentNode != root) {
            // Removing this node leaves the parent with one child.
            // If the parent doesn't hold a key or if it isn't the root,
            // we can merge it with its single child node.
            assert (edgeIndex < 2);
            RadixNode otherChild = parentNode.firstNode();
            if (edgeIndex == 0) {
                otherChild = parentNode.secondNode();
            }
            // Append the child node's prefix to the parent node.
            parentNode.prefix(expand(parentNode.prefix(), otherChild.prefix()));
            // Copy the rest of child node's data to the parent node.
            parentNode.nodes(otherChild);
            parentNode.refCount(otherChild.refCount());

            return true;
        }
        // This is a leaf node that doesn't leave its parent with one
        // outgoing edge. Remove the outgoing edge to this node from the
        // parent.
        assert (outgoingEdges == 0);

        // Nothing points to this node now, so we can reclaim it.
        parentNode.remove(edgeIndex, currentNode.prefix(0), currentNode);

        return true;
    }

    private ByteBuffer expand(ByteBuffer first, ByteBuffer second)
    {
        ByteBuffer buffer = second.duplicate();
        assert (first.position() < second.position());
        assert secondOverlapsFirst(first, second);
        // the key of the second node already holds the beginning of the first node's prefix.
        // we just need to rewind the position of the second key to the position of the first one.
        buffer.position(first.position());
        return buffer;
    }

    private boolean secondOverlapsFirst(ByteBuffer first, ByteBuffer second)
    {
        boolean rc = true;
        for (int idx = first.position(); idx < second.position(); ++idx) {
            rc &= first.get(idx) == second.get(idx);
        }
        return rc;
    }

    /**
     * Check whether particular key is in the tree.
     *
     * @param key the key to check.
     * @return true if the key is in the tree, otherwise false.
     */
    boolean check(ByteBuffer key)
    {
        if (root.refCount() > 0) {
            return true;
        }
        assert (key != null) : "A key cannot be null in a radix tree";
        MatchResult result = match(key, true);
        int keySize = key.remaining();
        return result.keyBytesMatched == keySize
                && result.prefixBytesMatched == result.currentNode.prefixLength()
                && result.currentNode.refCount() > 0;
    }

    /**
     * Apply the function supplied to each key in the tree.
     */
    <K, T> void apply(BiConsumer<K, T> function, Function<List<ByteBuffer>, K> mapper, T arg)
    {
        root.visitKeys(new LinkedList<>(), function, mapper, arg);
    }
}
