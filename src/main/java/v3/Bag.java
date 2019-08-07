package v3;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class Bag<T> implements Iterable<T> {

    Node<T> first;
    int n;

    public Bag() {
        first = null;
        n = 0;
    }

    public boolean isEmpty() {
        return first == null;
    }

    public int size() {
        return n;
    }

    public void add(T item) {
        Node<T> oldfirst = first;
        first = new Node<T>();
        first.item = item;
        first.next = oldfirst;
        n++;
    }

    public Iterator<T> iterator() {
        return new ListIterator<>(first);
    }

    public RelationshipExt getFirstUnvisitedOutgoingEdge() {
        Node<T> current = this.first;
        while (current != null) {
            if (!((RelationshipExt)current.item).visited) {
                return (RelationshipExt)current.item;
            } else {
                current = current.next;
            }
        }
        return null;
    }

    public static class Node<Item> {
        Item item;
        Node<Item> next;

        @Override
        public String toString() {
            return "Node{" +
                    "item=" + item +
                    '}';
        }
    }

    public class ListIterator<Item> implements Iterator<Item> {

        private Node<Item> current;

        public ListIterator(Node<Item> first) {
            current = first;
        }

        public boolean hasNext() {
            return current != null;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        public Item next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Item item = current.item;
            current = current.next;
            return item;
        }
    }

}