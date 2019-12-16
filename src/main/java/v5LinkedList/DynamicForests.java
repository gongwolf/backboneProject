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
            if (t.SpTree.contains(rel_id)) {
                return true;
            }

        }
        return false;
    }


    public boolean deleteEdge(Relationship r) {
        int e_level = (int) r.getProperty("level");
        Relationship rel = null;
        boolean canBeDeleted = false;

        System.out.println("Size of the Forests " + this.dforests.size() + "   edge at level : " + e_level);
//        if(r.getId()==104L) {
//            SpanningTree level1_tree = this.dforests.get(1).trees.get(1);
//            level1_tree.printETTree();
//            level1_tree.firstOccurrences.forEach((k,v)->System.out.println("Item : " + k + " Count : " + v.data));
//            level1_tree.lastOccurrences.forEach((k,v)->System.out.println("Item : " + k + " Count : " + v.data));
//            level1_tree.nodeFirstOccurrences.forEach((k,v)->System.out.println("Item : " + k + " Count : " + v.data));
//            level1_tree.nodeLastOccurrences.forEach((k,v)->System.out.println("Item : " + k + " Count : " + v.data));
//        }

        int l = e_level;
        for (; l >= 0; l--) {
            SpanningTree sp_tree = this.dforests.get(l).findTree(r);
            int sp_idx = this.dforests.get(l).findTreeIndex(r);

            System.out.println("--- Find the relationship " + r + " at level " + l + "   " + (sp_tree == null) + "     size of the trees:" + this.dforests.get(l).trees.size() + "  " + sp_idx);

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

            System.out.println("##### " + combine_nodes_list.size() + " (" + left_sub_tree.N_nodes.size() + " + " + right_sub_tree.N_nodes.size() + ") " + middle_sub_tree.N + " | " + left_sub_tree.N_nodes.size() + ", " + right_sub_tree.N_nodes.size() + " | " + left_sub_tree.N + ", " + right_sub_tree.N);
            rel = findReplacementEdge(middle_sub_tree, l, r, combine_nodes_list, casenumber, left_sub_tree, right_sub_tree);

            /** If a replacement edge is found, jump out the loop **/
            if (rel != null) {
                System.out.println("Find the replacement edge at level " + l);
                break;
            } else {
                System.out.println("There is no replacement edge at level " + l);
            }

        }

        int level_replacement = l; // if level_replacement == -1, means can not delete it.


        if (level_replacement != -1) {
            System.out.println("Beginning to delete the edge " + r);
            deleteEdgeFromSpanningTree(e_level, level_replacement, r, rel);
            canBeDeleted = true;
        } else {
            System.out.println("There is no replacement edge can be found @@@@@@@@@@@@@@@");
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

            this.dforests.get(l).trees.remove(sp_idx); //remove the original spanning tree

            SpanningTree[] splittedTrees = new SpanningTree[3];
            splittedTrees[0] = new SpanningTree(sp_tree.neo4j, false);
            splittedTrees[1] = new SpanningTree(sp_tree.neo4j, false);
            splittedTrees[2] = new SpanningTree(sp_tree.neo4j, false);

            int case_number = sp_tree.split(deleted_rel, splittedTrees);

            SpanningTree left_tree = splittedTrees[0];
            SpanningTree middle_tree = splittedTrees[1];
            SpanningTree right_tree = splittedTrees[2];

            left_tree.etTreeUpdateInformation();
            middle_tree.etTreeUpdateInformation();
            right_tree.etTreeUpdateInformation();

            System.out.println("deleting edge at level " + l + "  ##### " + left_tree.N + "  " + middle_tree.N + "   " + right_tree.N + "  (replacement edge level: " + level_replacement + " )  case_number:" + case_number);

//            sp_tree.removeEdge(deleted_rel.getId(), case_number);
            right_tree = combineSpanningTree(left_tree, right_tree, case_number);
            right_tree.etTreeUpdateInformation();

            /** when l > level_replacement, only needs to delete the del_rel (because replace_edge in lower forests)
             *  else, delete and replace with the replacement edge.
             */
            if (l <= level_replacement) {
                long mid_new_root_id = (middle_tree.N_nodes.contains(replacement_rel.getStartNodeId())) ? replacement_rel.getStartNodeId() : replacement_rel.getEndNodeId();
                long right_new_root_id = (right_tree.N_nodes.contains(replacement_rel.getStartNodeId())) ? replacement_rel.getStartNodeId() : replacement_rel.getEndNodeId();
                System.out.println("new root: " + mid_new_root_id + "(" + middle_tree.isEmpty + " " + middle_tree.isSingle + ")" + "  " + right_new_root_id+ "(" + right_tree.isEmpty + " " + right_tree.isSingle + ")" );
                if(deleted_rel.getId()==11946){
                    System.out.println(middle_tree.nodeFirstOccurrences.get(mid_new_root_id).data+"  "+middle_tree.ettree.head.data+"  "+middle_tree.ettree.tail.data);
                    middle_tree.printETTree();
                    System.out.println(right_tree.nodeFirstOccurrences.get(right_new_root_id).data+"  "+right_tree.ettree.head.data+"  "+right_tree.ettree.tail.data);
                    right_tree.printETTree();
                }
                right_tree.reroot(right_new_root_id);
                middle_tree.reroot(mid_new_root_id);
                connectTwoTreeByRel(middle_tree, right_tree, replacement_rel);
                middle_tree.etTreeUpdateInformation();
                this.dforests.get(l).trees.add(middle_tree);
            } else {
                if (!middle_tree.isSingle && !middle_tree.isEmpty) {
                    this.dforests.get(l).trees.add(middle_tree);
                    middle_tree.etTreeUpdateInformation();
                    System.out.println("add back the splitted middle tree to the level " + l + "  forest ");

                }

                if (!right_tree.isSingle && !right_tree.isEmpty) {
                    this.dforests.get(l).trees.add(right_tree);
                    right_tree.etTreeUpdateInformation();
                    System.out.println("add back the splitted right tree to the level " + l + "  forest ");

                }
            }
        }
    }

    private void connectTwoTreeByRel(SpanningTree middle_tree, SpanningTree right_tree, Relationship replacement_rel) {
        int middle_tree_root_id = middle_tree.isEmpty || middle_tree.isSingle ? Math.toIntExact(middle_tree.N_nodes.iterator().next()) : middle_tree.ettree.head.data.start_id;
        int right_tree_root_id = right_tree.isEmpty || right_tree.isSingle ? Math.toIntExact(right_tree.N_nodes.iterator().next()) : right_tree.ettree.head.data.start_id;

        System.out.println("Left root: " + middle_tree_root_id + "(" + middle_tree.isEmpty + " " + middle_tree.isSingle + ")" + "  Right Root:" + right_tree_root_id + "(" + right_tree.isEmpty + " " + right_tree.isSingle + ")");

        // new edge back from the root of the right_tree to the root of the middle tree
        RelationshipExt iter_edge = new RelationshipExt(replacement_rel, middle_tree_root_id, right_tree_root_id); // new edge back from the root of the right_tree to the root of the middle tree
        ListNode<RelationshipExt> node = new ListNode<>(iter_edge);
        middle_tree.ettree.append(node);

        if (!right_tree.isSingle && !right_tree.isEmpty) {
            middle_tree.ettree.tail.next = right_tree.ettree.head;
            right_tree.ettree.head.prev = middle_tree.ettree.tail;
            middle_tree.ettree.tail = right_tree.ettree.tail;
        }

        // new edge back from the root of the right_tree to the root of the middle tree
        RelationshipExt back_iter_edge = new RelationshipExt(replacement_rel, right_tree_root_id, middle_tree_root_id);
        ListNode<RelationshipExt> back_node = new ListNode<>(back_iter_edge);
        middle_tree.ettree.append(back_node);

        middle_tree.etTreeUpdateInformation();
    }

    private Relationship findReplacementEdge(SpanningTree middle_sub_tree, int current_level, Relationship r, HashSet<Long> combine_nodes_list, int casenumber, SpanningTree left_sub_tree, SpanningTree right_sub_tree) {
        Relationship rel;

        int upper_level = current_level + 1;
        SpanningForests upper_forests = this.dforests.get(upper_level);

        if ((combine_nodes_list.size()) <= middle_sub_tree.N) { // find the replacement induces to the left tree and the right tree

            if (upper_forests == null) {
                System.out.println("Create higher level forests at level " + upper_level + "  for left sub tree" + " , " + combine_nodes_list.size());
                upper_forests = new SpanningForests(current_level + 1);
            }

//            right_sub_tree = combineSpanningTree(left_sub_tree, right_sub_tree, casenumber);
//            right_sub_tree.etTreeUpdateInformation();

            System.out.println("push the right tree to level " + upper_level + "======>   Is a single tree ?   " + right_sub_tree.isSingle + "   " + right_sub_tree.isEmpty);
            System.out.println("(before) number of trees at level " + upper_level + " is " + upper_forests.trees.size());

            if (!right_sub_tree.isSingle && !right_sub_tree.isEmpty) {
                SpanningTree new_tree = new SpanningTree(left_sub_tree, right_sub_tree);
                new_tree.increaseEdgeLevel(current_level);
                upper_forests.putNewSpanningTree(new_tree);
                this.dforests.put(upper_level, upper_forests);
            } else if (right_sub_tree.isSingle) {
                System.out.println("right tree is a single tree, it contains the only node " + right_sub_tree.N_nodes.iterator().next());
            } else {
                System.out.println("The tree is single or empty, no need to be pushed to higher level");
            }

            System.out.println("(after) number of trees at level " + upper_level + " is " + upper_forests.trees.size());

            rel = findReplacementEdgeByNodes(combine_nodes_list, middle_sub_tree.N_nodes, current_level, r, right_sub_tree.neo4j);

            if (rel != null) {
                System.out.println("found the replacement edge (right tree)" + rel);
            }

        } else { // find the replacement induce to the middle tree

            if (upper_forests == null) {
                System.out.println("Create higher level forests at level " + upper_level + "  for right sub tree" + "  " + middle_sub_tree.N + " , " + combine_nodes_list.size());
                upper_forests = new SpanningForests(upper_level);
            }

            System.out.println("push the middle tree to level " + upper_level + "   Is a single tree ?   " + middle_sub_tree.isSingle + "   " + middle_sub_tree.isEmpty);
            System.out.println("(before) number of trees at level " + upper_level + " is " + upper_forests.trees.size());

            if (!middle_sub_tree.isSingle && !middle_sub_tree.isEmpty) {
                SpanningTree new_tree = new SpanningTree(middle_sub_tree);
                new_tree.increaseEdgeLevel(current_level);
                upper_forests.putNewSpanningTree(new_tree);
                this.dforests.put(upper_level, upper_forests);
            } else if (middle_sub_tree.isSingle) {
                System.out.println("middle tree is a single tree, it contains the only node " + middle_sub_tree.N_nodes.iterator().next());
            } else {
                System.out.println("The tree is single or empty, no need to be pushed to higher level");
            }

            System.out.println("(after) number of trees at level " + upper_level + " is " + upper_forests.trees.size());

            rel = findReplacementEdgeByNodes(middle_sub_tree.N_nodes, combine_nodes_list, current_level, r, middle_sub_tree.neo4j);

            if (rel != null) {
                System.out.println("found the replacement edge (middle tree)" + rel);
            }
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
//                left_tree.etTreeUpdateInformation(); //Todo: may can be simplified, at this time, it changes the structure of the linked list
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
     * @param current_level         find the edge with specific level
     * @param del                   the relationship that needs to be deleted
     * @param neo4j                 the neo4j object
     * @return if the replacement edge can be find, then return the replacement edge. else return null.
     */
    public Relationship findReplacementEdgeByNodes(HashSet<Long> self_nodes_list, HashSet<Long> other_tree_nodes_list, int current_level, Relationship del, Neo4jDB neo4j) {
        Relationship rel = null;
        try (Transaction tx = neo4j.graphDB.beginTx()) {
            for (long node_id : self_nodes_list) {
                Node n = neo4j.graphDB.getNodeById(node_id);
                Iterator<Relationship> rels_iter = n.getRelationships(Line.Linked, Direction.BOTH).iterator();
                while (rels_iter.hasNext()) {
                    Relationship next_rel = rels_iter.next();
                    int edge_level = (int) next_rel.getProperty("level");
                    if (edge_level == current_level && next_rel.getId() != del.getId()) {
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
