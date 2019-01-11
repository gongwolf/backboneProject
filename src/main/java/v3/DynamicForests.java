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
        SpanningTree sp_tree = this.dforests.get(level_r).findTree(r);
        TNode<RelationshipExt> min_node = sp_tree.findMinimum();
        System.out.println("minimum node +" + min_node.item);

        System.out.println(sp_tree.neo4j.DB_PATH);

        SpanningTree left_sub_tree = new SpanningTree(sp_tree.neo4j, false);
        TNode<RelationshipExt> firstSplitor = sp_tree.findLeftSubTree(min_node, r, left_sub_tree);
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        left_sub_tree.rbtree.root.print();
        left_sub_tree.fixIfSingle();
        System.out.println(left_sub_tree.N);

        System.out.println("==============================================================");

        SpanningTree middle_sub_tree = new SpanningTree(sp_tree.neo4j, false);
        TNode<RelationshipExt> secondSplitor = sp_tree.findMiddleSubTree(firstSplitor, r, middle_sub_tree);
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        middle_sub_tree.rbtree.root.print();
        middle_sub_tree.fixIfSingle();
        System.out.println(middle_sub_tree.N);


        System.out.println("==============================================================");
        SpanningTree right_sub_tree = new SpanningTree(sp_tree.neo4j, false);
        sp_tree.findRightSubTree(secondSplitor, right_sub_tree);
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        right_sub_tree.rbtree.root.print();
        right_sub_tree.fixIfSingle();
        System.out.println(right_sub_tree.N);

        left_sub_tree.combineTree(right_sub_tree);

        System.out.println("==============================================================");
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
            if (replacement_relationship != null) {
                updateDynamicInformation(right_sub_tree, level_r);
                addNewTreeEdge(replacement_relationship, level_r, left_sub_tree, right_sub_tree);
                return true;
            }
        }
        System.out.println("end of the replacement function call at " + level_r);
        return false;
    }

    private void addNewTreeEdge(Relationship replacement_relationship, int level_r, SpanningTree left_sub_tree, SpanningTree right_sub_tree) {
        for (int i = level_r; i >= 0; i--) {
            long sid = replacement_relationship.getStartNodeId();
            long eid = replacement_relationship.getEndNodeId();
            System.out.println(replacement_relationship.getStartNodeId() + "~~~" + replacement_relationship.getEndNodeId());

            if (left_sub_tree.N_nodes.contains(sid)) {
                left_sub_tree.reroot(sid);
            } else {
                right_sub_tree.reroot(sid);
            }

            if (right_sub_tree.N_nodes.contains(sid)) {
                right_sub_tree.reroot(sid);
            } else {
                right_sub_tree.reroot(sid);
            }
        }
    }

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
        System.out.println("=======================merge the level " + new_level + " forest");
        dforests.get(new_level).merge();

    }
}
