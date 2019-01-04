package DataStructure;

import static DataStructure.STATIC.*;

public class TNode<T> {
    public T item = null;
    int key = -1;
    int color = BLACK;
    TNode<T> left = nil, right = nil, parent = nil;

    public TNode(int key, T item) {
        this.key = key;
        this.item = item;
    }

    public TNode() {
        this.key = -1;
    }


    public void print() {
        print("", true);
    }

    private void print(String prefix, boolean isTail) {
        System.out.println(prefix + (isTail ? "└── " : "├── ") + ((color == RED) ? "Color: Red " : "Color: Black ") +
                "Key: " + key + " content:" + ((item == null) ? "leaf" : item));
        if (item == null) return;
        if (left == nil && right == nil) return;
        left.print(prefix + (isTail ? "    " : "│   "), false);
        right.print(prefix + (isTail ? "    " : "│   "), true);
    }
}
