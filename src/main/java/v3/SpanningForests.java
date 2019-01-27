package v3;

import DataStructure.TNode;
import javafx.util.Pair;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.HashSet;

import static DataStructure.STATIC.nil;

public class SpanningForests {
    public ArrayList<SpanningTree> trees = new ArrayList<>();
    public int level;

    public SpanningForests(int level) {
        this.level = level;
    }

    public SpanningTree findTree(Relationship r) {
        for (SpanningTree current_tree : trees) {
            if (current_tree.hasEdge(r)) {
//                System.out.println("find spanning tree whose contains the edge " + r);
                return current_tree;
            }
        }
        return null;
    }


    public int findTreeIndex(Relationship r) {
        for (int i = 0; i < trees.size(); i++) {
            SpanningTree current_tree = trees.get(i);
            if (current_tree.hasEdge(r)) {
                System.out.println("the index of the spanning tree whose contains the edge " + r + " is " + i);
                return i;
            }
        }
        return -1;
    }


    public void addNewTrees(SpanningTree sptree) {
        trees.add(sptree);
    }

    public boolean merge(int level_r) {
        if (trees.size() == 1) {
            SpanningTree sub_tree = trees.get(0);
            updateTreePointers(sub_tree, level_r);
            System.out.println("There is only one tree in level " + level_r + ", do not need to merge");
        }

        Pair<Integer, Integer> tree_idx;
        while ((tree_idx = hasCouldMergedTree()) != null) {
            int i = tree_idx.getKey();
            int j = tree_idx.getValue();

            SpanningTree new_tree = mergeTree(trees.get(i), trees.get(j), level_r);
            System.out.println("finished merge in one iteration   with the tree " + i + " and tree " + j);
            /**
             * because i is always less than j, i is deleted before j.
             * After deletion of tree i, the index of tree j needs to decrease 1.
             * **/
            trees.remove(i);

            trees.remove(j - 1);

//            new_tree.printNodes();

            trees.add(new_tree);

        }
        return false;
    }


    private void updateTreePointers(SpanningTree sub_tree, int new_level) {
        TNode<RelationshipExt> min_node = sub_tree.findMinimum();
        sub_tree.updateRelationshipRBPointer(min_node.item, min_node.key, min_node.key, new_level);

        TNode<RelationshipExt> suc_node = sub_tree.rbtree.successor(min_node);
        while (suc_node != nil) {
            sub_tree.updateRelationshipRBPointer(suc_node.item, suc_node.key, suc_node.key, new_level);
            suc_node = sub_tree.rbtree.successor(suc_node);
        }
    }

    private SpanningTree mergeTree(SpanningTree spanningTree, SpanningTree spanningTree1, int level_r) {
        SpanningTree new_tree = new SpanningTree(spanningTree.neo4j, false);
        new_tree.N_nodes.addAll(spanningTree.N_nodes);
        new_tree.N_nodes.addAll(spanningTree1.N_nodes);

        new_tree.SpTree.addAll(spanningTree.SpTree);
        new_tree.SpTree.addAll(spanningTree1.SpTree);

        new_tree.N = new_tree.N_nodes.size();

        new_tree.clearRelationshipRBPointerAtLevel(level_r);

        int random_start_node_id = (int) (Math.random() * new_tree.N);
        new_tree.FindAdjList();


        ArrayList<Long> temp_node_list = new ArrayList<>(new_tree.N_nodes);
        int start_node_id = Math.toIntExact(temp_node_list.get(random_start_node_id));

        new_tree.FindEulerTourString(start_node_id, level_r);

        return new_tree;

    }

    private Pair<Integer, Integer> hasCouldMergedTree() {
        if (trees.size() == 1) {
            return null;
        }
        for (int i = 0; i < trees.size(); i++) {
            for (int j = i + 1; j < trees.size(); j++) {
                if (hasCommonNodes(trees.get(i), trees.get(j))) {
                    return new Pair<>(i, j);
                }
            }
        }
        return null;
    }

    private boolean hasCommonNodes(SpanningTree spanningTree, SpanningTree spanningTree1) {
        HashSet<Long> nodes_1 = spanningTree.N_nodes;
        HashSet<Long> nodes_2 = spanningTree1.N_nodes;
        for (long nid1 : nodes_1) {
            if (nodes_2.contains(nid1)) {
                return true;
            }
        }
        return false;
    }

    public void findTrees(Relationship replacement_relationship, SpanningTree left_sub_tree, SpanningTree right_sub_tree) {
        boolean findLeft = false, findRight = false;
        for (SpanningTree t : trees) {
            if (findLeft && findRight) {
                return;
            }
            if (t.N_nodes.contains(replacement_relationship.getStartNodeId())) {
                left_sub_tree = t;
                findLeft = true;
            } else if (t.N_nodes.contains(replacement_relationship.getEndNodeId())) {
                right_sub_tree = t;
                findRight = true;
            }
        }
    }

    public void deleteEdge(Relationship delete_edge) {
        System.out.println("deleted edge  " + delete_edge + " in forest at " + level);

        int sp_tree_idx = findTreeIndex(delete_edge);
        SpanningTree sp_tree = trees.get(sp_tree_idx);
        //Find the tree that contains given relationship r in the level level_r
        System.out.println("whole tree:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        sp_tree.rbtree.root.print();

        //Find the minimum node, the first node|relationship of the Euler tour of the spanning tree sp_tree
        TNode<RelationshipExt> min_node = sp_tree.findMinimum();
//        System.out.println("minimum node +" + min_node.item);
//        System.out.println(sp_tree.neo4j.DB_PATH);

        //Find the sub euler tour from the first until before the given r
        SpanningTree left_sub_tree = new SpanningTree(sp_tree.neo4j, false);
        System.out.println("min node " + min_node.item);
        TNode<RelationshipExt> firstSplitor = sp_tree.findLeftSubTree(min_node, delete_edge, left_sub_tree);
        System.out.println("left tree:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        left_sub_tree.rbtree.root.print();

        //Find the sub euler tour from the first given r to the second given r
        //if r == (v,w), first r means (v,w) or (w,v), the second r means the reverse of the first r, such as (w,v) or (v,w)
        SpanningTree middle_sub_tree = new SpanningTree(sp_tree.neo4j, false);
        TNode<RelationshipExt> secondSplitor = sp_tree.findMiddleSubTree(firstSplitor, delete_edge, middle_sub_tree);
        System.out.println("middle tree:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        middle_sub_tree.rbtree.root.print();

        //Find the sub euler tour from after the second r to the end of the Euler tour
        //Do not need to fix the right sub tree
        //If the right sub tree only have one edge, it means it only the return edge to the left sub tree
        SpanningTree right_sub_tree = new SpanningTree(sp_tree.neo4j, false);
        sp_tree.findRightSubTree(secondSplitor, right_sub_tree);
        System.out.println("right tree:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        right_sub_tree.rbtree.root.print();

        combination(left_sub_tree, middle_sub_tree, right_sub_tree, firstSplitor, secondSplitor);
        System.out.println("================  combination ============================");
        left_sub_tree.rbtree.root.print();
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        right_sub_tree.rbtree.root.print();
        System.out.println("==========================================================");

        left_sub_tree.printEdges();
        left_sub_tree.printNodes();
        System.out.println("---------------------------------------");
        right_sub_tree.printEdges();
        right_sub_tree.printNodes();
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println("left sub tree size : " + left_sub_tree.N + "     right sub tree size : " + right_sub_tree.N);



        //remove the tree that contains the deleted edge at first
        this.trees.remove(sp_tree_idx);
        if (!left_sub_tree.isSingle) this.trees.add(left_sub_tree);
        if (!right_sub_tree.isSingle) this.trees.add(right_sub_tree);

        this.merge(level);
    }

    public void replaceEdge(Relationship delete_edge, Relationship replacement_edge) {
        System.out.println("replace the edge  " + delete_edge + " with  " + replacement_edge + " in forest at " + level);

        int sp_tree_idx = findTreeIndex(delete_edge);
        SpanningTree sp_tree = trees.get(sp_tree_idx);

        //Find the tree that contains given relationship r in the level level_r
        System.out.println("whole tree:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        sp_tree.rbtree.root.print();

        //Find the minimum node, the first node|relationship of the Euler tour of the spanning tree sp_tree
        TNode<RelationshipExt> min_node = sp_tree.findMinimum();
//        System.out.println("minimum node +" + min_node.item);
//        System.out.println(sp_tree.neo4j.DB_PATH);

        //Find the sub euler tour from the first until before the given r
        SpanningTree left_sub_tree = new SpanningTree(sp_tree.neo4j, false);
        System.out.println("min node " + min_node.item);
        TNode<RelationshipExt> firstSplitor = sp_tree.findLeftSubTree(min_node, delete_edge, left_sub_tree);
        System.out.println("left tree:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        left_sub_tree.rbtree.root.print();

        //Find the sub euler tour from the first given r to the second given r
        //if r == (v,w), first r means (v,w) or (w,v), the second r means the reverse of the first r, such as (w,v) or (v,w)
        SpanningTree middle_sub_tree = new SpanningTree(sp_tree.neo4j, false);
        TNode<RelationshipExt> secondSplitor = sp_tree.findMiddleSubTree(firstSplitor, delete_edge, middle_sub_tree);
        System.out.println("middle tree:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        middle_sub_tree.rbtree.root.print();

        //Find the sub euler tour from after the second r to the end of the Euler tour
        //Do not need to fix the right sub tree
        //If the right sub tree only have one edge, it means it only the return edge to the left sub tree
        SpanningTree right_sub_tree = new SpanningTree(sp_tree.neo4j, false);
        sp_tree.findRightSubTree(secondSplitor, right_sub_tree);
        System.out.println("right tree:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        right_sub_tree.rbtree.root.print();

        combination(left_sub_tree, middle_sub_tree, right_sub_tree, firstSplitor, secondSplitor);
        System.out.println("================  combination ============================");
        left_sub_tree.rbtree.root.print();
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        right_sub_tree.rbtree.root.print();
        System.out.println("==========================================================");

        left_sub_tree.printEdges();
        left_sub_tree.printNodes();
        System.out.println("---------------------------------------");
        right_sub_tree.printEdges();
        right_sub_tree.printNodes();
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println("left sub tree size : " + left_sub_tree.N + "     right sub tree size : " + right_sub_tree.N);

        long sid = replacement_edge.getStartNodeId();
        long eid = replacement_edge.getEndNodeId();
        System.out.println(replacement_edge.getStartNodeId() + "~~~" + replacement_edge.getEndNodeId());


        if (left_sub_tree.N_nodes.contains(sid)) {
            System.out.println("re-root left sub-tree on node " + sid);
            left_sub_tree.reroot(sid, eid, level);
            System.out.println("re-root right sub-tree on node " + eid);
            right_sub_tree.reroot(eid, sid, level);
            connectTwoTree(left_sub_tree, right_sub_tree, replacement_edge, level);
            System.out.println("************************************************");
//                left_sub_tree.rbtree.root.print();
        } else {
            System.out.println("re-root left sub-tree on node " + eid);
            left_sub_tree.reroot(eid, sid, level);
            System.out.println("re-root right sub-tree on node " + sid);
            right_sub_tree.reroot(sid, eid, level);
            connectTwoTree(left_sub_tree, right_sub_tree, replacement_edge, level);
            System.out.println("************************************************");
//                left_sub_tree.rbtree.root.print();
        }
        trees.remove(sp_tree_idx);
        trees.add(left_sub_tree);

    }

    public void combination(SpanningTree left_sub_tree, SpanningTree middle_sub_tree, SpanningTree right_sub_tree, TNode<RelationshipExt> firstSplitor, TNode<RelationshipExt> secondSplitor) {
        System.out.println("Call combination function " + left_sub_tree.isEmpty() + "  " + middle_sub_tree.isEmpty() + "  " + right_sub_tree.isEmpty());
        if (left_sub_tree.isEmpty() && middle_sub_tree.isEmpty() && right_sub_tree.isEmpty()) {
            System.out.println("case1: left, middle and right sub tree is empty, create two single tree.");
            left_sub_tree.initializedAsSingleTree(firstSplitor.item.start_id);
            right_sub_tree.initializedAsSingleTree(firstSplitor.item.end_id);
        } else if (!left_sub_tree.isEmpty() && middle_sub_tree.isEmpty() && !right_sub_tree.isEmpty()) {
            System.out.println("case2: left and right sub tree is non-empty, middle tree is a empty tree. " +
                    "create a single node tree whose id is not same as the minimum node of the middle tree");
            int middle_tree_min_node_id = middle_sub_tree.findMinimum().item.start_id;
            int nodeid = firstSplitor.item.start_id == middle_tree_min_node_id ? firstSplitor.item.end_id : firstSplitor.item.start_id;
            left_sub_tree.initializedAsSingleTree(nodeid);
            right_sub_tree.copyTree(middle_sub_tree);
        } else if (left_sub_tree.isEmpty() && !middle_sub_tree.isEmpty() && right_sub_tree.isEmpty()) {
            System.out.println("case3: left and right sub tree are empty, middle tree is a non-empty tree. " +
                    "create a single node tree whose id is the end_id of the first splitter");
            int nodeid = firstSplitor.item.end_id;
            middle_sub_tree.initializedAsSingleTree(nodeid);
            left_sub_tree.combineTree(right_sub_tree);
            right_sub_tree.copyTree(middle_sub_tree);
        } else if (middle_sub_tree.isEmpty()) {
            int nodeid = firstSplitor.item.end_id;
            middle_sub_tree.initializedAsSingleTree(nodeid);
            if (left_sub_tree.isEmpty() && !right_sub_tree.isEmpty()) {
                System.out.println("case 4.1 left and middle tree are empty, right tree is a non-empty tree.");
                left_sub_tree.copyTree(right_sub_tree);
                right_sub_tree.copyTree(middle_sub_tree);
            } else if (!left_sub_tree.isEmpty() && right_sub_tree.isEmpty()) {
                System.out.println("case 4.2 middle and right tree are empty, left tree is a non-empty tree.");
                right_sub_tree.copyTree(middle_sub_tree);
            }
        } else if (!middle_sub_tree.isEmpty()) {
            if (!left_sub_tree.isEmpty() && right_sub_tree.isEmpty()) {
                System.out.println("case 5.1 left and middle tree are non0empty, right tree is a empty tree.");
                right_sub_tree.copyTree(middle_sub_tree);
            } else if (left_sub_tree.isEmpty() && !right_sub_tree.isEmpty()) {
                System.out.println("case 5.2 middle and right tree are empty, left tree is a empty tree.");
                left_sub_tree.copyTree(right_sub_tree);
                right_sub_tree.copyTree(middle_sub_tree);
            } else {
                System.out.println("case 6, left, middle and right are non-empty tree");
                left_sub_tree.combineTree(right_sub_tree);
                right_sub_tree.copyTree(middle_sub_tree);
            }
        }
    }

    private void connectTwoTree(SpanningTree left_sub_tree, SpanningTree right_sub_tree, Relationship replacement_edge, int level) {
        System.out.println("connecting two tree at level " + level);
        int src_id = (int) replacement_edge.getStartNodeId();
        int dest_id = (int) replacement_edge.getEndNodeId();


        int left_id = -1, right_id = -1;

        if (left_sub_tree.N_nodes.contains((long) src_id)) {
            System.out.println(src_id + " is in the left tree");
            left_id = src_id;
        } else if (right_sub_tree.N_nodes.contains((long) src_id)) {
            System.out.println(src_id + " is in the right tree");
            right_id = src_id;
        } else {
            System.out.println(src_id + " is not in neither the left nor the right tree");
        }


        if (left_sub_tree.N_nodes.contains((long) dest_id)) {
            System.out.println(dest_id + " is in the left tree");
            left_id = dest_id;
        } else if (right_sub_tree.N_nodes.contains((long) dest_id)) {
            System.out.println(dest_id + " is in the right tree");
            right_id = dest_id;
        } else {
            System.out.println(dest_id + " is not in neither the left nor the right tree");
        }

        if (left_id == -1 || right_id == -1) {
            System.err.println("Can not find src or dest neither in the left tree nor in the right tree");
            System.exit(0);
        }


        RelationshipExt rel_ext = new RelationshipExt(replacement_edge, left_id, right_id);
        RelationshipExt reverse_rel_ext = new RelationshipExt(replacement_edge, right_id, left_id);

        int max_key;
        max_key = left_sub_tree.findMaximumKeyValue();

        System.out.println(max_key);

        //insert first appears of replacement_relationship into the tree
        TNode<RelationshipExt> sNode = new TNode<>(++max_key, rel_ext);
        left_sub_tree.insert(sNode);
        left_sub_tree.updateRelationshipRBPointer(sNode.item, sNode.key, -1, level);


        if (!right_sub_tree.isSingle) {
            TNode<RelationshipExt> min_node = right_sub_tree.findMinimum();
            TNode<RelationshipExt> node = new TNode<>(min_node);
            int org_key = node.key;
            node.key = ++max_key;


            left_sub_tree.insert(node);
            left_sub_tree.updateRelationshipRBPointer(node.item, node.key, org_key, level);


            TNode<RelationshipExt> suc_node = right_sub_tree.rbtree.successor(min_node);
            while (suc_node != nil) {
                node = new TNode<>(suc_node);
                org_key = node.key;
                node.key = ++max_key;

                left_sub_tree.insert(node);
                left_sub_tree.updateRelationshipRBPointer(node.item, node.key, org_key, level);
                suc_node = right_sub_tree.rbtree.successor(suc_node);
            }


        }

        TNode<RelationshipExt> eNode = new TNode<>(++max_key, reverse_rel_ext);
        left_sub_tree.insert(eNode);
        left_sub_tree.updateRelationshipRBPointer(eNode.item, eNode.key, -1, level);

        right_sub_tree.rbtree.root = nil; //empty right_tree
    }
}
