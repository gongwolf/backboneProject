package DataStructure;

import static DataStructure.STATIC.*;

public class TNode<T> {
    int key = -1;
    public T item = null;
    int color = BLACK;
    TNode<T> left = nil, right = nil, parent = nil;

    public TNode(int key, T item) {
        this.key = key;
        this.item = item;
    }

    public TNode() {
        this.key = -1;
    }
}
