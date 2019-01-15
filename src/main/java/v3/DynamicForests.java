package v3;

import DataStructure.TNode;
import org.neo4j.graphdb.Relationship;

import java.util.HashMap;

public class DynamicForests {
    HashMap<Integer, SpanningForests> dforests;

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
     * Todo: May could be optimized by finding the highest level where the given edge @rel belongs to .
     *
     * @param rel given edge
     * @return if the given edge rel is the tree edge return true, otherwise return false.
     */
    public boolean isTreeEdge(Relationship rel) {
        SpanningForests sp = this.dforests.get(0);

        for (SpanningTree t : sp.trees) {
            for (Relationship sp_tree_edge : t.SpTree) {
                if (sp_tree_edge.getId() == rel.getId()) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean replacement(Relationship r, int level_r) {
        //Find the tree that contains given relationship r in the level level_r
        SpanningTree sp_tree = this.dforests.get(level_r).findTree(r);

        //Find the minimum node, the first node|relationship of the Euler tour of the spanning tree sp_tree
        TNode<RelationshipExt> min_node = sp_tree.findMinimum();
        System.out.println("minimum node +" + min_node.item);
        System.out.println(sp_tree.neo4j.DB_PATH);

        //Find the sub euler tour from the first until before the given r
        SpanningTree left_sub_tree = new SpanningTree(sp_tree.neo4j, false);
        TNode<RelationshipExt> firstSplitor = sp_tree.findLeftSubTree(min_node, r, left_sub_tree);
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        left_sub_tree.rbtree.root.print();
        left_sub_tree.fixIfSingle();
        System.out.println(left_sub_tree.N);
        System.out.println("==============================================================");

        //Find the sub euler tour from the first given r to the second given r
        //if r == (v,w), first r means (v,w) or (w,v), the second r means the reverse of the first r, such as (w,v) or (v,w)
        SpanningTree middle_sub_tree = new SpanningTree(sp_tree.neo4j, false);
        TNode<RelationshipExt> secondSplitor = sp_tree.findMiddleSubTree(firstSplitor, r, middle_sub_tree);
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        middle_sub_tree.rbtree.root.print();
        middle_sub_tree.fixIfSingle();
        System.out.println(middle_sub_tree.N);
        System.out.println("==============================================================");

        //Find the sub euler tour from after the second r to the end of the Euler tour
        SpanningTree right_sub_tree = new SpanningTree(sp_tree.neo4j, false);
        sp_tree.findRightSubTree(secondSplitor, right_sub_tree);
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        right_sub_tree.rbtree.root.print();
        right_sub_tree.fixIfSingle();
        System.out.println(right_sub_tree.N);
        left_sub_tree.combineTree(right_sub_tree); //combine left tree and right, the cutting process of the euler tree.
        System.out.println("==============================================================");

        /* test print the two trees*/
        left_sub_tree.rbtree.root.print();
        left_sub_tree.fixIfSingle();
        System.out.println(left_sub_tree.N);

        right_sub_tree = middle_sub_tree;
        right_sub_tree.rbtree.root.print();
        right_sub_tree.fixIfSingle();
        System.out.println(right_sub_tree.N);


        //update edge level in the smaller tree
        if (left_sub_tree.N < right_sub_tree.N) {
            left_sub_tree.updateTreeEdgeLevel(level_r);
            left_sub_tree.findReplacementEdge(right_sub_tree, level_r);
        } else {
            Relationship replacement_relationship = right_sub_tree.findReplacementEdge(left_sub_tree, level_r);
            System.out.println("Found replacement relationship " + replacement_relationship);
            if (replacement_relationship != null) {
                updateDynamicInformation(right_sub_tree, level_r); //push the right tree to higher level
                addNewTreeEdge(replacement_relationship, r, level_r); // add new edge from level 0 to level_r forests
                return true;
            }
        }
        System.out.println("end of the replacement function call at " + level_r);
        return false;
    }

    /**
     * Add new edge to all the forests whose level is equal or lower than level_r.
     * Update level_r's spanning tree by connect two sub tree by given replacement relationship
     *
     * @param replacement_relationship
     * @param r
     * @param level_r
     */
    private void addNewTreeEdge(Relationship replacement_relationship, Relationship r, int level_r) {
        for (int i = level_r; i >= 0; i--) {
            long sid = replacement_relationship.getStartNodeId();
            long eid = replacement_relationship.getEndNodeId();
            System.out.println(replacement_relationship.getStartNodeId() + "~~~" + replacement_relationship.getEndNodeId());

            SpanningTree sp_tree = this.dforests.get(level_r).findTree(r);

            TNode<RelationshipExt> min_node = sp_tree.findMinimum();
            System.out.println("minimum node +" + min_node.item);
            System.out.println(sp_tree.neo4j.DB_PATH);

            //Find the sub euler tour from the first until before the given r
            SpanningTree left_sub_tree = new SpanningTree(sp_tree.neo4j, false);
            TNode<RelationshipExt> firstSplitor = sp_tree.findLeftSubTree(min_node, r, left_sub_tree);
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
            left_sub_tree.rbtree.root.print();
            left_sub_tree.fixIfSingle();
            System.out.println(left_sub_tree.N);
            System.out.println("==============================================================");

            //Find the sub euler tour from the first given r to the second given r
            //if r == (v,w), first r means (v,w) or (w,v), the second r means the reverse of the first r, such as (w,v) or (v,w)
            SpanningTree middle_sub_tree = new SpanningTree(sp_tree.neo4j, false);
            TNode<RelationshipExt> secondSplitor = sp_tree.findMiddleSubTree(firstSplitor, r, middle_sub_tree);
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
            middle_sub_tree.rbtree.root.print();
            middle_sub_tree.fixIfSingle();
            System.out.println(middle_sub_tree.N);
            System.out.println("==============================================================");

            //Find the sub euler tour from after the second r to the end of the Euler tour
            SpanningTree right_sub_tree = new SpanningTree(sp_tree.neo4j, false);
            sp_tree.findRightSubTree(secondSplitor, right_sub_tree);
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
            right_sub_tree.rbtree.root.print();
            right_sub_tree.fixIfSingle();
            System.out.println(right_sub_tree.N);
            left_sub_tree.combineTree(right_sub_tree); //combine left tree and right, the cutting process of the euler tree.
            left_sub_tree.fixIfSingle();

            right_sub_tree = middle_sub_tree;
            right_sub_tree.fixIfSingle();

            if (left_sub_tree.N_nodes.contains(sid)) {
                System.out.println("re-root left sub-tree on node " + sid);
                left_sub_tree.reroot(sid);
                System.out.println("re-root right sub-tree on node " + eid);
                right_sub_tree.reroot(eid);
                connectTwoTree(left_sub_tree, right_sub_tree, replacement_relationship);
            } else {
                left_sub_tree.reroot(eid);
                right_sub_tree.reroot(sid);
            }

        }
    }


    //Todo: needs to be finished
    private void connectTwoTree(SpanningTree left_sub_tree, SpanningTree right_sub_tree, Relationship replacement_relationship) {
        int src_id = (int) replacement_relationship.getStartNodeId();
        int dest_id = (int) replacement_relationship.getEndNodeId();
        RelationshipExt rel_ext = new RelationshipExt(replacement_relationship, src_id, dest_id);
        RelationshipExt reverse_rel_ext = new RelationshipExt(replacement_relationship, dest_id, src_id);

        int max_key = left_sub_tree.findMaximumKeyValue();
        TNode<RelationshipExt> min_node = left_sub_tree.findMinimum();
    }

    /**
     * Push sub_tree to the higher level spanning forest
     *
     * @param sub_tree the given sub_tree that wants to be added to higher level
     * @param level_r  the given level forest that the sub_tree needs to be added
     */
    private void updateDynamicInformation(SpanningTree sub_tree, int level_r) {
        sub_tree.updateTreeEdgeLevel(level_r);
        int new_level = level_r + 1;

        if (dforests.containsKey(new_level)) {
            dforests.get(new_level).addNewTrees(sub_tree);
        } else {
            SpanningForests sp = new SpanningForests(new_level);
            System.out.println("Create new level " + new_level + " forests");
            sp.trees.add(sub_tree);
            dforests.put(new_level, sp);
        }

        System.out.println("put new right spanning tree to level " + new_level + " forests");
        System.out.println("Starting to merge the level " + new_level + " forest ............. ");
        dforests.get(new_level).merge();
        System.out.println("Finished merge the level " + new_level + " forest .............");

    }
}
