package v5LinkedList;

import Neo4jTools.Neo4jDB;
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


    public void deleteEdge(Relationship r, Neo4jDB neo4j) {
        int e_level = (int) r.getProperty("level");
        System.out.println("Find the relationship " + r + " at level " + e_level);
        boolean foundReplacement = false;

        for (int l = e_level; l >= 0; l--) {
            SpanningTree sp_tree = this.dforests.get(l).findTree(r);

            SpanningTree[] splittedTrees = new SpanningTree[2];
            splittedTrees[0] = new SpanningTree(sp_tree.neo4j, false);
            splittedTrees[1] = new SpanningTree(sp_tree.neo4j, false);

            sp_tree.split(r, splittedTrees);
        }

    }
}
