package DataStructure;

public class RedBlackTree<T> {
    public final TNode nil = new TNode();
    private final int RED = 0;
    private final int BLACK = 1;
    public TNode<T> root = nil;

    public boolean insert(TNode<T> newnode) {

        newnode.color = RED;
        newnode.left = nil;
        newnode.right = nil;

        TNode<T> y = nil; //parent of x
        TNode<T> x = this.root;

        //find proper parent y of new node
        while (x != nil) {
            y = x;
            if (newnode.key < x.key) {
                x = x.left;
            } else {
                x = x.right;
            }
        }

        newnode.parent = y;
        if (y == nil) {
            root = newnode;
        } else if (newnode.key < y.key) {
            y.left = newnode;
        } else {
            y.right = newnode;
        }

        insertFIXUP(newnode);

        return true;
    }

    private void insertFIXUP(TNode<T> newnode) {
        TNode<T> zNode = newnode;
        while (zNode.parent.color == RED) {
            //if the parent of the new node is a left child
            if (zNode.parent == zNode.parent.parent.left) {
                TNode<T> uncleNode = zNode.parent.parent.right;
                //case 1. zNode's uncle is red
                if (uncleNode.color == RED) {
                    zNode.parent.color = BLACK;
                    uncleNode.color = BLACK;
                    zNode.parent.parent.color = RED;
                    zNode = zNode.parent.parent;
                } else {
                    //case'2, zNode's uncle is black
                    if (zNode == zNode.parent.right) {
                        zNode = zNode.parent;
                        leftRotation(zNode);
                    }

                    //case'3
                    zNode.parent.color = BLACK;
                    zNode.parent.parent.color = RED;
                    rightRotation(zNode.parent.parent);
                }
            } else {
                TNode<T> uncleNode = zNode.parent.parent.left;
                //case 1. zNode's uncle is red
                if (uncleNode.color == RED) {
                    zNode.parent.color = BLACK;
                    uncleNode.color = BLACK;
                    zNode.parent.parent.color = RED;
                    zNode = zNode.parent.parent;
                } else {
                    //case'2, zNode's uncle is black
                    if (zNode == zNode.parent.left) {
                        zNode = zNode.parent;
                        rightRotation(zNode);
                    }

                    //case'3
                    zNode.parent.color = BLACK;
                    zNode.parent.parent.color = RED;
                    leftRotation(zNode.parent.parent);
                }
            }
        }

        root.color = BLACK;
    }

    private void leftRotation(TNode<T> x) {
        TNode<T> y = x.right;
        x.right = y.left;
        if (y.left != nil) {
            y.left.parent = x;
        }
        y.parent = x.parent;
        if (x.parent == nil) {
            root = y;
        } else if (x.parent.left == x) {
            x.parent.left = y;
        } else {
            x.parent.right = y;
        }

        y.left = x;
        x.parent = y;
    }


    private void rightRotation(TNode<T> x) {
        TNode<T> y = x.left;
        x.left = y.right;
        if (y.right != nil) {
            y.right.parent = x;
        }
        y.parent = x.parent;
        if (x.parent == nil) {
            root = y;
        } else if (x.parent.left == x) {
            x.parent.left = y;
        } else {
            x.parent.right = y;
        }
        y.right = x;
        x.parent = y;
    }


    /**
     * replace u with v
     *
     * @param u target node
     * @param v with node
     */
    private void transplant(TNode<T> u, TNode<T> v) {
        if (u.parent == nil) {
            root = v;
        } else if (u.parent.left == u) {
            u.parent.left = v;
        } else {
            u.parent.right = v;
        }
        v.parent = u.parent;
    }

    private void delete(TNode<T> node) {
        TNode<T> y = node;
        int y_org_color = y.color;
        TNode<T> x;
        if (node.left == nil) {
            x = node.right;
            transplant(node, node.right);
        } else if (node.right == nil) {
            x = node.left;
            transplant(node, node.left);
        }
    }

    class TNode<T> {
        int key = -1;
        T item = null;
        int color = BLACK;
        TNode<T> left = nil, right = nil, parent = nil;

        TNode(int key, T item) {
            this.key = key;
            this.item = item;
        }

        public TNode() {
            this.key = -1;
        }
    }


}

