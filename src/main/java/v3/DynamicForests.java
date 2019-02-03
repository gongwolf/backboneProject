package v3;

import DataStructure.TNode;
import org.neo4j.graphdb.Relationship;

import java.util.HashMap;



public class DynamicForests {
    public HashMap<Integer, SpanningForests> dforests;

    public DynamicForests() {
        dforests = new HashMap<>();
    }

    public void createBase(SpanningTree sptree_base) {
        int initLevel = 0;
        SpanningForests sp = new SpanningForests(initLevel);
        sp.trees.add(sptree_base);
        dforests.put(initLevel, sp);
    }

    /**
     * Check whether the edge is a tree edge in the first level of spanning trees.
     *
     * @param rel given edge
     * @return if the given edge rel is the tree edge return true, otherwise return false.
     */
    public boolean isTreeEdge(Relationship rel) {
        SpanningForests sp = this.dforests.get(0);
        for (SpanningTree t : sp.trees) {
            for (long rid: t.SpTree) {
                Relationship sp_tree_edge=t.neo4j.graphDB.getRelationshipById(rid);
                if (sp_tree_edge.getId() == rel.getId()) {
                    return true;
                }
            }
        }
        return false;
    }

    public Relationship replacement(Relationship r, int level_r) {
        //Find the tree that contains given relationship r in the level level_r
        SpanningTree sp_tree = this.dforests.get(level_r).findTree(r);
        //Find the minimum node, the first node|relationship of the Euler tour of the spanning tree sp_tree
        TNode<RelationshipExt> min_node = sp_tree.findMinimum();

        //Find the sub euler tour from the first until before the given r
        SpanningTree left_sub_tree = new SpanningTree(sp_tree.neo4j, false);
        TNode<RelationshipExt> firstSplitor = sp_tree.findLeftSubTree(min_node, r, left_sub_tree);

        //Find the sub euler tour from the first given r to the second given r
        //if r == (v,w), first r means (v,w) or (w,v), the second r means the reverse of the first r, such as (w,v) or (v,w)
        SpanningTree middle_sub_tree = new SpanningTree(sp_tree.neo4j, false);
        TNode<RelationshipExt> secondSplitor = sp_tree.findMiddleSubTree(firstSplitor, r, middle_sub_tree);

        //Find the sub euler tour from after the second r to the end of the Euler tour
        //Do not need to fix the right sub tree
        //If the right sub tree only have one edge, it means it only the return edge to the left sub tree
        SpanningTree right_sub_tree = new SpanningTree(sp_tree.neo4j, false);
        sp_tree.findRightSubTree(secondSplitor, right_sub_tree);

        combination(left_sub_tree, middle_sub_tree, right_sub_tree, firstSplitor, secondSplitor);
//        System.out.println("left sub tree size : " + left_sub_tree.N + "     right sub tree size : " + right_sub_tree.N);


        //update edge level in the smaller tree
        if (left_sub_tree.N < right_sub_tree.N) {
            updateDynamicInformation(left_sub_tree, level_r); //push the left tree to higher level
            Relationship replacement_relationship = left_sub_tree.findReplacementEdge(right_sub_tree, level_r, r);
//            System.out.println("end of the replacement function call at " + level_r);
            return replacement_relationship;
        } else {
            updateDynamicInformation(right_sub_tree, level_r); //push the right tree to higher level
            Relationship replacement_relationship = right_sub_tree.findReplacementEdge(left_sub_tree, level_r, r);
//            System.out.println("end of the replacement function call at " + level_r);
            return replacement_relationship;
        }
    }

    public void combination(SpanningTree left_sub_tree, SpanningTree middle_sub_tree, SpanningTree right_sub_tree, TNode<RelationshipExt> firstSplitor, TNode<RelationshipExt> secondSplitor) {
//        System.out.println("Call combination function " + left_sub_tree.isEmpty() + "  " + middle_sub_tree.isEmpty() + "  " + right_sub_tree.isEmpty());
        if (left_sub_tree.isEmpty() && middle_sub_tree.isEmpty() && right_sub_tree.isEmpty()) {
//            System.out.println("case1: left, middle and right sub tree is empty, create two single tree.");
            left_sub_tree.initializedAsSingleTree(firstSplitor.item.start_id);
            right_sub_tree.initializedAsSingleTree(firstSplitor.item.end_id);
        } else if (!left_sub_tree.isEmpty() && middle_sub_tree.isEmpty() && !right_sub_tree.isEmpty()) {
//            System.out.println("case2: left and right sub tree is non-empty, middle tree is a empty tree. " +
//                    "create a single node tree whose id is not same as the minimum node of the right tree");
            int nodeid = firstSplitor.item.end_id;
            middle_sub_tree.initializedAsSingleTree(nodeid);
            left_sub_tree.combineTree(right_sub_tree);
            right_sub_tree.copyTree(middle_sub_tree);
        } else if (left_sub_tree.isEmpty() && !middle_sub_tree.isEmpty() && right_sub_tree.isEmpty()) {
//            System.out.println("case3: left and right sub tree are empty, middle tree is a non-empty tree. " +
//                    "create a single node tree whose id is the end_id of the first splitter");
            int nodeid = firstSplitor.item.start_id;
            left_sub_tree.initializedAsSingleTree(nodeid);
            right_sub_tree.copyTree(middle_sub_tree);
        } else if (middle_sub_tree.isEmpty()) {
//            System.out.println("case 4:");
            int nodeid = firstSplitor.item.end_id;
            middle_sub_tree.initializedAsSingleTree(nodeid);
            if (left_sub_tree.isEmpty() && !right_sub_tree.isEmpty()) {
//                System.out.println("case 4.1 left and middle tree are empty, right tree is a non-empty tree.");
                left_sub_tree.copyTree(right_sub_tree);
                right_sub_tree.copyTree(middle_sub_tree);
            } else if (!left_sub_tree.isEmpty() && right_sub_tree.isEmpty()) {
//                System.out.println("case 4.2 middle and right tree are empty, left tree is a non-empty tree.");
                right_sub_tree.copyTree(middle_sub_tree);
            }
        } else if (!middle_sub_tree.isEmpty()) {
            if (!left_sub_tree.isEmpty() && right_sub_tree.isEmpty()) {
//                System.out.println("case 5.1 left and middle tree are non0empty, right tree is a empty tree.");
                right_sub_tree.copyTree(middle_sub_tree);
            } else if (left_sub_tree.isEmpty() && !right_sub_tree.isEmpty()) {
//                System.out.println("case 5.2 middle and right tree are empty, left tree is a empty tree.");
                left_sub_tree.copyTree(right_sub_tree);
                right_sub_tree.copyTree(middle_sub_tree);
            } else {
//                System.out.println("case 6, left, middle and right are non-empty tree");
                left_sub_tree.combineTree(right_sub_tree);
                right_sub_tree.copyTree(middle_sub_tree);
            }
        }
    }


    /**
     * Push sub_tree to the higher level spanning forest
     *
     * @param sub_tree the given sub_tree that wants to be added to higher level
     * @param level_r  the given level forest that the sub_tree needs to be added
     */
    private void updateDynamicInformation(SpanningTree sub_tree, int level_r) {

        //If it's a single node, do not need to push to higher level
        //update pointer of edge <-> TNode to higher level
        if (sub_tree.isSingle) {
//            System.out.println("the sub tree is a single tree, do not need to push to higher level (level " + (level_r + 1) + ")");
            return;
        } else {
            sub_tree.updateTreeEdgeLevel(level_r);
            int new_level = level_r + 1;
            sub_tree.updateTreePointers(new_level);

            if (dforests.containsKey(new_level)) {
                dforests.get(new_level).addNewTrees(sub_tree);
            } else {
                SpanningForests sp = new SpanningForests(new_level);
//                System.out.println("Create new level " + new_level + " forests");
                sp.trees.add(sub_tree);
                dforests.put(new_level, sp);
            }

//            System.out.println("put new spanning tree to level " + new_level + " forests");
//            System.out.println("Starting to merge the level " + new_level + " forest ............. ");
            dforests.get(new_level).merge(new_level);
//            System.out.print("Finished merge the level " + new_level + " forest .............");
//            System.out.println("There are  " + dforests.get(new_level).trees.size() + " trees in level " + new_level + " forest .............");
        }
    }
}
