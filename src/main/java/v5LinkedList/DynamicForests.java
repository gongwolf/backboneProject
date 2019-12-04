package v5LinkedList;

import org.neo4j.graphdb.Relationship;

import java.util.HashMap;

public class DynamicForests {

    public HashMap<Integer, SpanningForests> dforests;

    public DynamicForests() {
        dforests = new HashMap<>();
    }

    public void createBase(SpanningTree sptree_base) {
        /**
         * Create a spanning tree forests for level 0.
         * Put et-tree of the spanning tree of the original graph to it.
         */
        int initLevel = 0;
        SpanningForests sp = new SpanningForests(initLevel);
        sp.trees.add(sptree_base);
        dforests.put(initLevel, sp);
    }


    /**
     * Check whether the edge is a tree edge in the first level of spanning trees.
     *
     * @param rel_id The relationship id
     * @return if the given edge rel is the tree edge return true, otherwise return false.
     */
    public boolean isTreeEdge(long rel_id) {
        SpanningForests sp = this.dforests.get(0);
        for (SpanningTree t : sp.trees) {
            for (long rid : t.SpTree) {
                if (rid == rel_id) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Find the replacement edge of r in current level spanning forest.
     *
     * @param r       the deleted relatoinship
     * @param level_r the level of the specific spanning forests
     * @return if the replacement relationship is found, return it. If not, return null.
     */
    public Relationship replacement(Relationship r, int level_r) {
        SpanningTree sp_tree = this.dforests.get(level_r).findTree(r);

        /*** Split the spanning tree ***/
        SpanningTree[] splittedTrees = new SpanningTree[2];
        SpanningTree left_sub_tree = new SpanningTree(sp_tree.neo4j, false);
        SpanningTree right_sub_tree = new SpanningTree(sp_tree.neo4j, false);
        splittedTrees[0] = left_sub_tree;
        splittedTrees[1] = right_sub_tree;
        sp_tree.split(r, splittedTrees);

        return null;
    }


}
