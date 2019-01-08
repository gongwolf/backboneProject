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
        TNode<RelationshipExt> firstSplitor = sp_tree.findLeftSubTree(min_node, r);
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        TNode<RelationshipExt> secondSplitor = sp_tree.findMiddleSubTree(firstSplitor, r);
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        sp_tree.findRightSubTree(secondSplitor);


        return false;
    }
}
