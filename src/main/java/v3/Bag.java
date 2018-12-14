package v3;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class Bag implements Iterable<RelationshipExt> {

    private Node<RelationshipExt> first;
    private int n;

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

    public void add(RelationshipExt item) {
        Node<RelationshipExt> oldfirst = first;
        first = new Node<RelationshipExt>();
        first.item = item;
        first.next = oldfirst;
        n++;
    }

    public Iterator<RelationshipExt> iterator() {
        return new ListIterator<RelationshipExt>(first);
    }

    public RelationshipExt getFirstUnvisitedOutgoingEdge() {
        Node<RelationshipExt> current = this.first;
        while (current != null) {
            if (!current.item.visited) {
                return current.item;
            } else {
                current = current.next;
            }
        }
        return null;
    }

    private static class Node<Item> {

        private Item item;
        private Node<Item> next;
    }

    private class ListIterator<Item> implements Iterator<Item> {

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