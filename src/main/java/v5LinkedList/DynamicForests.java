package v5LinkedList;

import Neo4jTools.Line;
import Neo4jTools.Neo4jDB;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

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
        Relationship rel = null;

        System.out.println("Size of the Forests " + this.dforests.size());

        int l = e_level;
        for (; l >= 0; l--) {
            System.out.println("--- Find the relationship " + r + " at level " + l);

            SpanningTree sp_tree = this.dforests.get(l).findTree(r);
            /** Three sub-tree **/
            SpanningTree[] splittedTrees = new SpanningTree[3];
            splittedTrees[0] = new SpanningTree(sp_tree.neo4j, false);
            splittedTrees[1] = new SpanningTree(sp_tree.neo4j, false);
            splittedTrees[2] = new SpanningTree(sp_tree.neo4j, false);

            System.out.println("##### " + sp_tree.ettree.n + " = " + splittedTrees[0].ettree.n + " + " + splittedTrees[1].ettree.n + " + " + splittedTrees[2].ettree.n + " + 2 ? "
                    + ((splittedTrees[0].ettree.n + splittedTrees[1].ettree.n + splittedTrees[2].ettree.n + 2) == sp_tree.ettree.n));

            int case_number = sp_tree.split(r, splittedTrees);
            SpanningTree left_sub_tree = splittedTrees[0];
            SpanningTree middle_sub_tree = splittedTrees[1];
            SpanningTree right_sub_tree = splittedTrees[2];

            HashSet<Long> combine_nodes_list = new HashSet<>();
            combine_nodes_list.addAll(left_sub_tree.N_nodes);
            combine_nodes_list.addAll(right_sub_tree.N_nodes);

            System.out.println("##### " + sp_tree.ettree.n + " = " + splittedTrees[0].ettree.n + " + " + splittedTrees[1].ettree.n + " + " + splittedTrees[2].ettree.n + " + 2 ? "
                    + ((splittedTrees[0].ettree.n + splittedTrees[1].ettree.n + splittedTrees[2].ettree.n + 2) == sp_tree.ettree.n));
            System.out.println("##### " + combine_nodes_list.size() + "  " + middle_sub_tree.N + "   " + left_sub_tree.N_nodes.size() + " " + right_sub_tree.N_nodes.size() + " " + left_sub_tree.N + " " + right_sub_tree.N);

            rel = findReplacementEdge(middle_sub_tree, l, r, combine_nodes_list);

            /** If a replacement edge is found, jump out the loop **/
            if (rel != null) {
                break;
            }
        }

        int level_replacement = l; // if level_replacement == -1, means can not delete it.

        //Todo: Find the replacement edge, connect the sub-trees
        if (level_replacement != -1) {
            deleteEdgeFromSpanningTree(e_level, level_replacement, r, rel);
        }

        System.out.println("=======================================");

    }

    /***
     * If the replacement edge is found,
     * remove the edge in the forests level (e_level to 0 )
     * if the level is greater than l, just remove and split trees into Tv and Tu, do nothing
     * if the level is less or equal to l, remove the edges and
     *
     * @param edge_level level of the deleted edge
     * @param level_replacement the level where the replacement edge is found
     * @param deleted_rel the deleted edge
     * @param replacement_rel the replacement edge of the deleted edge
     */
    private void deleteEdgeFromSpanningTree(int edge_level, int level_replacement, Relationship deleted_rel, Relationship replacement_rel) {
        int l;
        for (l = edge_level; l >= 0; l--) {
            int sp_idx = this.dforests.get(l).findTreeIndex(deleted_rel);
            SpanningTree sp_tree = this.dforests.get(l).trees.get(sp_idx);
            this.dforests.get(l).trees.remove(sp_idx);

            SpanningTree[] splittedTrees = new SpanningTree[3];
            splittedTrees[0] = new SpanningTree(sp_tree.neo4j, false);
            splittedTrees[1] = new SpanningTree(sp_tree.neo4j, false);
            splittedTrees[2] = new SpanningTree(sp_tree.neo4j, false);

            int case_number = sp_tree.split(deleted_rel, splittedTrees);

            SpanningTree left_tree = splittedTrees[0];
            SpanningTree middle_tree = splittedTrees[1];
            SpanningTree right_tree = splittedTrees[2];

            sp_tree.removeEdge(deleted_rel.getId()); //Todo: Implement it
            right_tree = combineSpanningTree(left_tree, right_tree, case_number);

            if (l <= level_replacement) {
                middle_tree.reroot(replacement_rel.getStartNodeId());//Todo: implement it
                right_tree.reroot(replacement_rel.getEndNodeId());

                connectTwoTreeByRel(middle_tree, right_tree, replacement_rel); //Todo: implement it
            }

        }
    }

    private void connectTwoTreeByRel(SpanningTree middle_tree, SpanningTree right_tree, Relationship replacement_rel) {
    }

    private Relationship findReplacementEdge(SpanningTree middle_sub_tree, int l, Relationship r, HashSet<Long> combine_nodes_list) {
        Relationship rel = null;

        //Todo: If find the replacement edge, merge left and right subtree, create a hard copy of the smaller tree to higher level and push to the higher level
        if ((combine_nodes_list.size()) <= middle_sub_tree.N) { // find the replacement induces to the left tree and the right tree
            SpanningForests upper_forests = this.dforests.get(l + 1);
            if (upper_forests == null) {
                System.out.println("Create higher level forests " + (l + 1) + "  for left sub tree" + "  " + combine_nodes_list.size());
                upper_forests = new SpanningForests(l + 1);
            }

        } else { // find the replacement induce to the middle tree
            SpanningForests upper_forests = this.dforests.get(l + 1);
            if (upper_forests == null) {
                System.out.println("Create higher level forests " + (l + 1) + "  for right sub tree" + "  " + middle_sub_tree.N + "  " + combine_nodes_list.size());
                upper_forests = new SpanningForests(l + 1);
            }

            middle_sub_tree.ettree.createNewCopy();
            middle_sub_tree.increaseEdgeLevel();

            upper_forests.putNewSpanningTree(middle_sub_tree); //Todo: Merge with existed tree in l+1 level

            rel = findReplacementEdgeByNodes(middle_sub_tree.N_nodes, combine_nodes_list, l, r, middle_sub_tree.neo4j);

            if (rel != null) {
                System.out.println("found the replacement edge " + rel);
            }

            int upper_level = l + 1;
            this.dforests.put(upper_level, upper_forests);
        }

        return rel;
    }

    private SpanningTree combineSpanningTree(SpanningTree left_tree, SpanningTree right_tree, int case_number) {
        switch (case_number) {
            case 1:
            case 3:
            case 4:
            case 6:
                return left_tree;
            case 2:
            case 8:
                left_tree.ettree.tail.next = right_tree.ettree.head;
                right_tree.ettree.head.prev = left_tree.ettree.tail;
                right_tree.ettree.head = left_tree.ettree.head;
                left_tree.ettree.tail = right_tree.ettree.tail;
                left_tree.etTreeUpdateInformation(); //Todo: may can be simplified
                return left_tree;
            case 5:
            case 7:
                return right_tree;
        }
        return null;
    }

    /**
     * @param self_nodes_list       the node list of the tree itself
     * @param other_tree_nodes_list the node list of another tree
     * @param level                 find the edge with specific level
     * @param del                   the relationship that needs to be deleted
     * @param neo4j                 the neo4j object
     * @return if the replacement edge can be find, then return the replacement edge. else return null.
     */
    public Relationship findReplacementEdgeByNodes(HashSet<Long> self_nodes_list, HashSet<Long> other_tree_nodes_list, int level, Relationship del, Neo4jDB neo4j) {
        Relationship rel = null;
        try (Transaction tx = neo4j.graphDB.beginTx()) {
            for (long node_id : self_nodes_list) {
                Node n = neo4j.graphDB.getNodeById(node_id);
                Iterator<Relationship> rels_iter = n.getRelationships(Line.Linked, Direction.BOTH).iterator();
                while (rels_iter.hasNext()) {
                    Relationship next_rel = rels_iter.next();
                    int edge_level = (int) next_rel.getProperty("level");
                    if (edge_level == level && next_rel.getId() != del.getId()) {
                        if (other_tree_nodes_list.contains(next_rel.getOtherNodeId(node_id))) {
                            rel = next_rel;
                            return rel;
                        } else {
                            next_rel.setProperty("level", edge_level + 1); //Increase the edge level by 1
                        }
                    }
                }
            }
            tx.success();
        }
        return rel;
    }
}
