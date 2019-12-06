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
        boolean foundReplacement = false;

        for (int l = e_level; l >= 0; l--) {
            System.out.println("--- Find the relationship " + r + " at level " + l);
            SpanningTree sp_tree = this.dforests.get(l).findTree(r);

            SpanningTree[] splittedTrees = new SpanningTree[3];
            splittedTrees[0] = new SpanningTree(sp_tree.neo4j, false);
            splittedTrees[1] = new SpanningTree(sp_tree.neo4j, false);
            System.out.println("##### "+sp_tree.ettree.n+" = "+splittedTrees[0].ettree.n+" + "+splittedTrees[1].ettree.n+" + 2 ? "+((splittedTrees[0].ettree.n+splittedTrees[1].ettree.n+2)==sp_tree.ettree.n));
            sp_tree.split(r, splittedTrees);
            SpanningTree left_sub_tree = splittedTrees[0];
            SpanningTree right_sub_tree = splittedTrees[1];
            System.out.println("##### "+sp_tree.ettree.n+" = "+splittedTrees[0].ettree.n+" + "+splittedTrees[1].ettree.n+" + 2 ? "+((splittedTrees[0].ettree.n+splittedTrees[1].ettree.n+2)==sp_tree.ettree.n));

            //Todo: If find the replacement edge, merge left and right subtree, create a hard copy of the smaller tree to higher level and push to the higher level
            if(left_sub_tree.N_nodes.size() <= right_sub_tree.N_nodes.size()){
                SpanningForests upper_forests = this.dforests.get(l + 1);
                if(upper_forests==null){
                    System.out.println("Create higher level forests "+(l+1)+"  for left sub tree");
                }
            }else {
                SpanningForests upper_forests = this.dforests.get(l + 1);
                if(upper_forests==null){
                    System.out.println("Create higher level forests "+(l+1)+"  for right sub tree");
                    upper_forests = new SpanningForests(l+1);
                    //Todo: Create a copy of the new tree, especially the et-tree
                    upper_forests.trees.add(right_sub_tree);
                    dforests.put(l+1, upper_forests);
                }
                
            }

        }

        System.out.println("=======================================");

    }
}
