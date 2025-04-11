package db.buffer;

import java.util.HashMap;
import java.util.Map;

public class LRUReplacement {
    private final Map<Buffer, Node> replacementBufferMap;
    private Node head;
    private Node tail;

    public LRUReplacement() {
        this.head = null;
        this.tail = null;
        this.replacementBufferMap = new HashMap<>();
    }

    public void insert(Buffer buffer) {
        Node node = new Node(buffer);
        if (null != tail) {
            tail.next = node;
        }
        node.prev = tail;
        tail = node;

        if (null == head) {
            head = node;
        }

        replacementBufferMap.put(buffer, node);
    }

    public Buffer get() {
        if (null == head) {
            return null;
        }

        return head.buffer;
    }

    public void remove(Buffer buffer) {
        Node node = replacementBufferMap.get(buffer);
        if (null == node) {
            return;
        }
        if (null != node.prev) {
            node.prev.next = node.next;
        }
        if (null != node.next) {
            node.next.prev = node.prev;
        }

        if (head == node) {
            head = node.next;
        }

        replacementBufferMap.remove(buffer);
    }

    private static class Node {
        private final Buffer buffer;
        private Node next;
        private Node prev;

        public Node(Buffer buffer) {
            this.buffer = buffer;
            this.next = null;
            this.prev = null;
        }
    }
}
