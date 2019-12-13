package v5LinkedList;

import DataStructure.ListNode;
import DataStructure.RelationshipExt;
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


    public boolean deleteEdge(Relationship r) {
        int e_level = (int) r.getProperty("level");
        Relationship rel = null;
        boolean canBeDeleted = false;

        System.out.println("Size of the Forests " + this.dforests.size());

        int l = e_level;
        for (; l >= 0; l--) {
            SpanningTree sp_tree = this.dforests.get(l).findTree(r);

            System.out.println("--- Find the relationship " + r + " at level " + l + "   " + (sp_tree == null) + "     size of the trees:" + this.dforests.get(l).trees.size());
//            if (l == 1) {
//                for (SpanningTree sp : this.dforests.get(l).trees) {
//                    System.out.println(sp.N + "   " + sp.SpTree.size());
//                    sp.printETTree();
//                    System.out.println("=================================");
//                }
//            }

            /** Three sub-tree **/
            SpanningTree[] splittedTrees = new SpanningTree[3];
            splittedTrees[0] = new SpanningTree(sp_tree.neo4j, false);
            splittedTrees[1] = new SpanningTree(sp_tree.neo4j, false);
            splittedTrees[2] = new SpanningTree(sp_tree.neo4j, false);

//            System.out.println("##### " + sp_tree.ettree.n + " = " + splittedTrees[0].ettree.n + " + " + splittedTrees[1].ettree.n + " + " + splittedTrees[2].ettree.n + " + 2 ? "
//                    + ((splittedTrees[0].ettree.n + splittedTrees[1].ettree.n + splittedTrees[2].ettree.n + 2) == sp_tree.ettree.n));

            int casenumber = sp_tree.split(r, splittedTrees);
            SpanningTree left_sub_tree = splittedTrees[0];
            SpanningTree middle_sub_tree = splittedTrees[1];
            SpanningTree right_sub_tree = splittedTrees[2];

            HashSet<Long> combine_nodes_list = new HashSet<>();
            combine_nodes_list.addAll(left_sub_tree.N_nodes);
            combine_nodes_list.addAll(right_sub_tree.N_nodes);

            System.out.println("##### " + combine_nodes_list.size() + "  " + middle_sub_tree.N + " | " + left_sub_tree.N_nodes.size() + ", " + right_sub_tree.N_nodes.size() + " | " + left_sub_tree.N + ", " + right_sub_tree.N);
            rel = findReplacementEdge(middle_sub_tree, l, r, combine_nodes_list, casenumber, left_sub_tree, right_sub_tree);

            /** If a replacement edge is found, jump out the loop **/
            if (rel != null) {
                System.out.println("Find the replacement edge at level " + l);
                break;
            }
        }

        int level_replacement = l; // if level_replacement == -1, means can not delete it.

        if (level_replacement != -1) {
            deleteEdgeFromSpanningTree(e_level, level_replacement, r, rel);
            canBeDeleted = true;
        }

        System.out.println("=======================================");

        return canBeDeleted;
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
            System.out.println("deleting edge at level " + l + "  ##### " + left_tree.N + "  " + middle_tree.N + "   " + right_tree.N + "  (replacement edge level: " + level_replacement + " )");

            sp_tree.removeEdge(deleted_rel.getId(), case_number);
            right_tree = combineSpanningTree(left_tree, right_tree, case_number);

            /** when l >= level_replacement, only needs to delete the del_rel (because replace_edge in lower forests)
             *  else, delete and replace with the replacement edge.
             */
            if (l <= level_replacement) {
                long mid_new_root_id = (middle_tree.N_nodes.contains(replacement_rel.getStartNodeId())) ? replacement_rel.getStartNodeId() : replacement_rel.getEndNodeId();
                long right_new_root_id = (right_tree.N_nodes.contains(replacement_rel.getStartNodeId())) ? replacement_rel.getStartNodeId() : replacement_rel.getEndNodeId();
                System.out.println("new root: " + mid_new_root_id + "  " + right_new_root_id);
                middle_tree.reroot(mid_new_root_id);
                right_tree.reroot(right_new_root_id);
                connectTwoTreeByRel(middle_tree, right_tree, replacement_rel);
                middle_tree.etTreeUpdateInformation();
                this.dforests.get(l).trees.add(middle_tree);
            } else {
                if (!middle_tree.isSingle && !middle_tree.isEmpty) {
                    this.dforests.get(l).trees.add(middle_tree);

                }

                if (!right_tree.isSingle && !right_tree.isEmpty) {
                    this.dforests.get(l).trees.add(right_tree);

                }
            }
        }
    }

    private void connectTwoTreeByRel(SpanningTree middle_tree, SpanningTree right_tree, Relationship replacement_rel) {
        int middle_tree_root_id = middle_tree.ettree.head.data.start_id;
        int right_tree_root_id = right_tree.ettree.head.data.start_id;

        middle_tree.ettree.tail.next = right_tree.ettree.head;
        right_tree.ettree.head.prev = middle_tree.ettree.tail;
        middle_tree.ettree.tail = right_tree.ettree.tail;

        RelationshipExt iter_edge = new RelationshipExt(replacement_rel, right_tree_root_id, middle_tree_root_id); // new edge back from the root of the right_tree to the root of the middle tree
        ListNode<RelationshipExt> node = new ListNode<>(iter_edge);
        middle_tree.ettree.append(node);
        middle_tree.etTreeUpdateInformation();
//        System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
//        System.out.println(middle_tree.ettree.head.data + "     " + middle_tree.ettree.tail.data);
//        System.out.println(right_tree.ettree.head.data + "     " + right_tree.ettree.tail.data);
//        System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
    }

    private Relationship findReplacementEdge(SpanningTree middle_sub_tree, int l, Relationship r, HashSet<Long> combine_nodes_list, int casenumber, SpanningTree left_sub_tree, SpanningTree right_sub_tree) {
        Relationship rel;

        if ((combine_nodes_list.size()) <= middle_sub_tree.N) { // find the replacement induces to the left tree and the right tree
            int upper_level = l + 1;

            SpanningForests upper_forests = this.dforests.get(l + 1);
            if (upper_forests == null) {
                System.out.println("Create higher level forests " + (l + 1) + "  for left sub tree" + " , " + combine_nodes_list.size());
                upper_forests = new SpanningForests(l + 1);
            }

            right_sub_tree = combineSpanningTree(left_sub_tree, right_sub_tree, casenumber);
            System.out.println("push the right tree ======>   Is a single tree ?   " + right_sub_tree.isSingle + "   " + right_sub_tree.isEmpty);

            if (!right_sub_tree.isSingle && !right_sub_tree.isEmpty) {
                right_sub_tree.ettree.createNewCopy();
                right_sub_tree.etTreeUpdateInformation();
                right_sub_tree.increaseEdgeLevel();
                upper_forests.putNewSpanningTree(right_sub_tree);
            } else {
                System.out.println("The tree is single or empty, no need to be pushed to higher level");
            }

            System.out.println("number of trees at level " + upper_level + " is " + upper_forests.trees.size());

            rel = findReplacementEdgeByNodes(combine_nodes_list, middle_sub_tree.N_nodes, l, r, right_sub_tree.neo4j);

            if (rel != null) {
                System.out.println("found the replacement edge " + rel);
            }

            this.dforests.put(upper_level, upper_forests);
        } else { // find the replacement induce to the middle tree
            int upper_level = l + 1;

            SpanningForests upper_forests = this.dforests.get(upper_level);
            if (upper_forests == null) {
                System.out.println("Create higher level forests " + upper_level + "  for right sub tree" + "  " + middle_sub_tree.N + " , " + combine_nodes_list.size());
                upper_forests = new SpanningForests(upper_level);
            }


            System.out.println("push the middle tree ======>   Is a single tree ?   " + middle_sub_tree.isSingle + "   " + middle_sub_tree.isEmpty);

            if (!middle_sub_tree.isSingle && !middle_sub_tree.isEmpty) {
                middle_sub_tree.ettree.createNewCopy();
                middle_sub_tree.etTreeUpdateInformation();
                middle_sub_tree.increaseEdgeLevel();
                upper_forests.putNewSpanningTree(middle_sub_tree);
            } else if (middle_sub_tree.isSingle) {
                System.out.println("middle tree is a single tree, it contains the only node " + middle_sub_tree.N_nodes.iterator().next());
            } else {
                System.out.println("The tree is single or empty, no need to be pushed to higher level");
            }

            System.out.println("number of trees at level " + upper_level + " is " + upper_forests.trees.size());

            rel = findReplacementEdgeByNodes(middle_sub_tree.N_nodes, combine_nodes_list, l, r, middle_sub_tree.neo4j);

            if (rel != null) {
                System.out.println("found the replacement edge " + rel);
            }

            this.dforests.put(upper_level, upper_forests);

//            System.out.println("**************************");
//            if (l == 0) {
//                for (SpanningTree sp : this.dforests.get(upper_level).trees) {
//                    System.out.println(upper_level+"  "+ sp.N + "   " + sp.SpTree.size());
//                    sp.printETTree();
//                    System.out.println("=================================");
//                }
//            }
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
