package DataStructure;

import static DataStructure.STATIC.*;

public class RedBlackTree<T> {
    public TNode<T> root = nil;

    public boolean insert(int key, T item) {
        TNode<T> node = new TNode<>(key, item);
        return insert(node);
    }

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
            } else if(newnode.key>x.key){
                x = x.right;
            }else {
                System.err.println("the same key exception");
                System.exit(0);
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

    public boolean delete(int key, T item) {
        TNode<T> node = new TNode<>(key, item);
        return delete(node);
    }


    public boolean delete(int key) {
        TNode<T> node = findNodeByKey(key, root);
        return delete(node);
    }

    public boolean delete(TNode<T> node) {
        if ((node = findNode(node, root)) == null) return false;
//        System.out.println("delete " + node.key + "   " + (node.color == RED ? "RED" : "BLACK") + "  " + node.parent.key);
        TNode<T> x;
        TNode<T> y = node;
        int y_org_color = y.color;

        //if either node's left or right child is null, delete node. so x point to the no-nil child of the node.
        //if node has two children, y is the successor of the node. so x point to the right child of y.
        //delete y, replace y with x.
        //replace z with y.
        if (node.left == nil) {
            x = node.right;
            transplant(node, node.right);
        } else if (node.right == nil) {
            x = node.left;
            transplant(node, node.left);
        } else {
            y = treeMinimun(node.right);
//            System.out.println("replace with  " + y.key + "   " + (y.color == RED ? "RED" : "BLACK") + "  " + y.parent.key);
            y_org_color = y.color;
            x = y.right;
//            if (x == nil) {
//                System.out.println("nil leaf node");
//            } else {
//                System.out.println("x:  " + x.key + "   " + (x.color == RED ? "RED" : "BLACK") + "  " + x.parent.key);
//            }

            if (y.parent == node) {
                x.parent = y;
            } else {
                transplant(y, y.right); //y's right child replace the place of y.
                y.right = node.right;
                y.right.parent = y;
            }
            transplant(node, y);
            y.left = node.left;
            y.left.parent = y;
            y.color = node.color;

        }
        if (y_org_color == BLACK) {
//            System.out.println("fix color");
            deleteFix(x);
        }

//        System.out.println("==============================");
        return false;
    }

    private void deleteFix(TNode<T> x) {
        while (x != root && x.color == BLACK) {
            if (x == x.parent.left) {
//                System.out.println("x is left child of its parent");
                TNode<T> w = x.parent.right; //sibling of x
                /** case 1: x's sibling is red **/
                if (w.color == RED) {
                    w.color = BLACK;
                    x.parent.color = RED;
                    leftRotation(x.parent);
                    w = x.parent.right;
                }
                /** after dealing with case 1, it transfers to case 2, 3 and 4**/
                if (w.left.color == BLACK && w.right.color == BLACK) {
                    /** case 2: w is black that has two black children **/
                    w.color = RED;
                    x = x.parent;
                    continue;
                } else if (w.right.color == BLACK) {
                    /** case 3: w is black that only the right child is black **/
                    w.left.color = BLACK;
                    w.color = RED;
                    rightRotation(w);
                    w = x.parent.right;
                }
                /** after dealing with case 3, it may transfer to case 4**/
                if (w.right.color == RED) {
                    /** case 4: w is black whose right child is red **/
                    w.color = x.parent.color;
                    x.parent.color = BLACK;
                    w.right.color = BLACK;
                    leftRotation(x.parent);
                    x = root;
                }
            } else if (x == x.parent.right) {
                TNode<T> w = x.parent.left; //sibling of x
//                System.out.println("x is right child of its parent");
                /** case 1: x's sibling is red **/
                if (w.color == RED) {
//                    System.out.println("case 1");
                    w.color = BLACK;
                    x.parent.color = RED;
                    rightRotation(x.parent);
                    w = x.parent.left;
                }
                /** after dealing with case 1, it transfers to case 2, 3 and 4**/
                if (w.left.color == BLACK && w.right.color == BLACK) {
                    /** case 2: w is black that has two black children **/
//                    System.out.println("case 2");
                    w.color = RED;
                    x = x.parent;
                    continue;
                } else if (w.left.color == BLACK) {
                    /** case 3: w is black that only the right child is black **/
                    w.right.color = BLACK;
                    w.color = RED;
                    leftRotation(w);
                    w = x.parent.left;
                }
                /** after dealing with case 3, it may transfer to case 4**/
                if (w.left.color == RED) {
                    /** case 4: w is black whose right child is red **/
                    w.color = x.parent.color;
                    x.parent.color = BLACK;
                    w.left.color = BLACK;
                    rightRotation(x.parent);
                    x = root;
                }
            }
        }
        x.color = BLACK;
    }

    private TNode<T> treeMinimun(TNode<T> node) {
        while (node.left != nil) {
            node = node.left;
        }
        return node;
    }

    public void printTree(TNode<T> node) {
        if (node == nil) {
            return;
        }
        printTree(node.left);
        System.out.print(((node.color == RED) ? "Color: Red " : "Color: Black ") +
                "Key: " + node.key + " Parent: " + node.parent.key +
                " content:" + node.item + "\n");
        printTree(node.right);
    }


    private TNode<T> findNode(TNode<T> findNode, TNode<T> node) {
        if (root == nil) {
            return null;
        }

        if (findNode.key < node.key) {
            if (node.left != nil) {
                return findNode(findNode, node.left);
            }
        } else if (findNode.key > node.key) {
            if (node.right != nil) {
                return findNode(findNode, node.right);
            }
        } else if (findNode.key == node.key) {
            return node;
        }
        return null;
    }


    private TNode<T> findNodeByKey(int key, TNode<T> node) {
        if (root == nil) {
            return null;
        }
        if (key < node.key) {
            if (node.left != nil) {
                return findNodeByKey(key, node.left);
            }
        } else if (key > node.key) {
            if (node.right != nil) {
                return findNodeByKey(key, node.right);
            }
        } else if (key == node.key) {
            return node;
        }
        return null;
    }

    public TNode<T> findMinimum(TNode<T> subroot) {
        TNode<T> n = subroot;
        while (n.left != nil) {
            n = n.left;
        }
        return n;
    }

    public TNode<T> successor(TNode<T> node){
        if(node.right!=nil){
            return findMinimum(node.right);
        }
        TNode<T> y = node.parent;
        while(y!=nil && node==y.right){
            node = y;
            y = y.parent;
        }

        return y;
    }

    public int findMaximumKeyValue(TNode<T> node) {
        while (node.right != nil) {
            node = node.right;
        }
        return node.key;
    }

    public int findMaximumKeyValueTracable(TNode<T> node) {
        while (node.right != nil) {
            System.out.println(node.key+"   "+node.item+" "+node.left.key+"("+(node.left != nil)+"):"+node.left.item
                    +"   "+node.right.key+"("+(node.right != nil)+"):"+node.right.item);
            node = node.right;
        }
        System.out.println(node.key+"   "+node.item+" "+node.left.key+"("+(node.left != nil)+"):"+node.left.item
                +"   "+node.right.key+"("+(node.right != nil)+"):"+node.right.item);
        return node.key;
    }
}

