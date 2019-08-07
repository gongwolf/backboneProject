package DataStructure;

import java.util.ArrayList;
import static DataStructure.STATIC.*;

public class RBTest {
    private RedBlackTree<Integer> redblacktree;

    public static void main(String args[]) {
        RBTest test = new RBTest();
        test.TreeTest();
        test.printTest();
    }

    private void printTest() {
        if (this.redblacktree.root == nil) {
            System.out.println("Thnis a empty red black tree");
        } else {
            this.redblacktree.printTree(redblacktree.root);
        }
    }

    private void TreeTest() {
        RedBlackTree<Integer> redblacktree = new RedBlackTree<>();
        this.redblacktree = redblacktree;

        ArrayList<Integer> keys = new ArrayList<>();
        keys.add(3);
        keys.add(7);
        keys.add(18);
        keys.add(10);
        keys.add(8);
        keys.add(11);
        keys.add(22);
        keys.add(26);

        for (int key : keys) {
            System.out.println("Insert the node " + key + "  !!");
            this.redblacktree.insert(key, key);
        }

        redblacktree.delete(7,7);

//        for (int i = 0; i < 7; i++) {
//            int key = ThreadLocalRandom.current().nextInt(1, 50 + 1);
//            if (!keys.contains(key)) {
//                keys.add(key);
//                System.out.println("Insert the node "+key+"  !!");
//                this.redblacktree.insert(key,key);
//            }
//        }

//        int deleteindex=ThreadLocalRandom.current().nextInt(0, keys.size());
//        int deletekey = keys.get(deleteindex);
//        redblacktree.delete(deletekey,deletekey);
    }
}
