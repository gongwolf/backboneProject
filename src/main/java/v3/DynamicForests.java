package v3;

import DataStructure.TNode;
import org.neo4j.graphdb.Relationship;

import java.util.HashMap;

import static DataStructure.STATIC.nil;


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
            for (Relationship sp_tree_edge : t.SpTree) {
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
        System.out.println("whole tree:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        sp_tree.rbtree.root.print();

        //Find the minimum node, the first node|relationship of the Euler tour of the spanning tree sp_tree
        TNode<RelationshipExt> min_node = sp_tree.findMinimum();
//        System.out.println("minimum node +" + min_node.item);
//        System.out.println(sp_tree.neo4j.DB_PATH);

        //Find the sub euler tour from the first until before the given r
        SpanningTree left_sub_tree = new SpanningTree(sp_tree.neo4j, false);
        System.out.println("min node " + min_node.item);
        TNode<RelationshipExt> firstSplitor = sp_tree.findLeftSubTree(min_node, r, left_sub_tree);
        System.out.println("left tree:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        left_sub_tree.rbtree.root.print();

        //Find the sub euler tour from the first given r to the second given r
        //if r == (v,w), first r means (v,w) or (w,v), the second r means the reverse of the first r, such as (w,v) or (v,w)
        SpanningTree middle_sub_tree = new SpanningTree(sp_tree.neo4j, false);
        TNode<RelationshipExt> secondSplitor = sp_tree.findMiddleSubTree(firstSplitor, r, middle_sub_tree);
        System.out.println("middle tree:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        middle_sub_tree.rbtree.root.print();

        //Find the sub euler tour from after the second r to the end of the Euler tour
        //Do not need to fix the right sub tree
        //If the right sub tree only have one edge, it means it only the return edge to the left sub tree
        SpanningTree right_sub_tree = new SpanningTree(sp_tree.neo4j, false);
        sp_tree.findRightSubTree(secondSplitor, right_sub_tree);
        System.out.println("right tree:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        right_sub_tree.rbtree.root.print();

        combination(left_sub_tree, middle_sub_tree, right_sub_tree, firstSplitor, secondSplitor);
        System.out.println("================  combination ============================");
        left_sub_tree.rbtree.root.print();
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        right_sub_tree.rbtree.root.print();
        System.out.println("==========================================================");

        left_sub_tree.printEdges();
        left_sub_tree.printNodes();
        System.out.println("---------------------------------------");
        right_sub_tree.printEdges();
        right_sub_tree.printNodes();
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println("left sub tree size : " + left_sub_tree.N + "     right sub tree size : " + right_sub_tree.N);


        //update edge level in the smaller tree
        if (left_sub_tree.N < right_sub_tree.N) {
            updateDynamicInformation(left_sub_tree, level_r); //push the left tree to higher level
            Relationship replacement_relationship = left_sub_tree.findReplacementEdge(right_sub_tree, level_r, r);
            System.out.println("end of the replacement function call at " + level_r);
            return replacement_relationship;
//            if (replacement_relationship != null) {
//                HashMap<Integer, Integer> keyUpdatesMap = new HashMap<>();
//                System.out.println("Found replacement relationship " + replacement_relationship + " and push left sub tree to higher level");
//                addNewTreeEdge(replacement_relationship, r, level_r, keyUpdatesMap); // add new edge from level 0 to level_r forests
//                return true;
//            }
        } else {
            updateDynamicInformation(right_sub_tree, level_r); //push the right tree to higher level
            Relationship replacement_relationship = right_sub_tree.findReplacementEdge(left_sub_tree, level_r, r);
            System.out.println("end of the replacement function call at " + level_r);
            return replacement_relationship;
//            if (replacement_relationship != null) {
//                HashMap<Integer, Integer> keyUpdatesMap = new HashMap<>();
//                System.out.println("Found replacement relationship " + replacement_relationship + " at level " + level_r);
//                addNewTreeEdge(replacement_relationship, r, level_r, keyUpdatesMap); // add new edge from level 0 to level_r forests
//                return true;
//            }
        }
    }

    public void combination(SpanningTree left_sub_tree, SpanningTree middle_sub_tree, SpanningTree right_sub_tree, TNode<RelationshipExt> firstSplitor, TNode<RelationshipExt> secondSplitor) {
        System.out.println("Call combination function " + left_sub_tree.isEmpty() + "  " + middle_sub_tree.isEmpty() + "  " + right_sub_tree.isEmpty());
        if (left_sub_tree.isEmpty() && middle_sub_tree.isEmpty() && right_sub_tree.isEmpty()) {
            System.out.println("case1: left, middle and right sub tree is empty, create two single tree.");
            left_sub_tree.initializedAsSingleTree(firstSplitor.item.start_id);
            right_sub_tree.initializedAsSingleTree(firstSplitor.item.end_id);
        } else if (!left_sub_tree.isEmpty() && middle_sub_tree.isEmpty() && !right_sub_tree.isEmpty()) {
            System.out.println("case2: left and right sub tree is non-empty, middle tree is a empty tree. " +
                    "create a single node tree whose id is not same as the minimum node of the right tree");
            int nodeid = firstSplitor.item.end_id;
            middle_sub_tree.initializedAsSingleTree(nodeid);
            left_sub_tree.combineTree(right_sub_tree);
            right_sub_tree.copyTree(middle_sub_tree);

        } else if (left_sub_tree.isEmpty() && !middle_sub_tree.isEmpty() && right_sub_tree.isEmpty()) {
            System.out.println("case3: left and right sub tree are empty, middle tree is a non-empty tree. " +
                    "create a single node tree whose id is the end_id of the first splitter");
//            int right_tree_min_node_id = right_sub_tree.findMinimum().item.start_id;
//            int nodeid = firstSplitor.item.start_id == right_tree_min_node_id ? firstSplitor.item.end_id : firstSplitor.item.start_id;
            int nodeid = firstSplitor.item.start_id;
            left_sub_tree.initializedAsSingleTree(nodeid);
            right_sub_tree.copyTree(middle_sub_tree);
        } else if (middle_sub_tree.isEmpty()) {
            System.out.println("case 4:");
            int nodeid = firstSplitor.item.end_id;
            System.out.println(nodeid);
            middle_sub_tree.initializedAsSingleTree(nodeid);
            if (left_sub_tree.isEmpty() && !right_sub_tree.isEmpty()) {
                System.out.println("case 4.1 left and middle tree are empty, right tree is a non-empty tree.");
                left_sub_tree.copyTree(right_sub_tree);
                right_sub_tree.copyTree(middle_sub_tree);
            } else if (!left_sub_tree.isEmpty() && right_sub_tree.isEmpty()) {
                System.out.println("case 4.2 middle and right tree are empty, left tree is a non-empty tree.");
                right_sub_tree.copyTree(middle_sub_tree);
            }
        } else if (!middle_sub_tree.isEmpty()) {
            if (!left_sub_tree.isEmpty() && right_sub_tree.isEmpty()) {
                System.out.println("case 5.1 left and middle tree are non0empty, right tree is a empty tree.");
                right_sub_tree.copyTree(middle_sub_tree);
            } else if (left_sub_tree.isEmpty() && !right_sub_tree.isEmpty()) {
                System.out.println("case 5.2 middle and right tree are empty, left tree is a empty tree.");
                left_sub_tree.copyTree(right_sub_tree);
                right_sub_tree.copyTree(middle_sub_tree);
            } else {
                System.out.println("case 6, left, middle and right are non-empty tree");
                left_sub_tree.combineTree(right_sub_tree);
                right_sub_tree.copyTree(middle_sub_tree);
            }
        }
    }

    /**
     * Add new edge to all the forests whose level is equal or lower than level_r.
     * Update level_r's spanning tree by connect two sub tree by given replacement relationship
     *
     * @param replacement_relationship
     * @param r
     * @param level_r
     */
    private void addNewTreeEdge(Relationship replacement_relationship, Relationship r, int level_r, HashMap<Integer, Integer> keyUpdatesMap) {
        for (int i = level_r; i >= 0; i--) {
            System.out.println("Add new edge on the level " + i);
            long sid = replacement_relationship.getStartNodeId();
            long eid = replacement_relationship.getEndNodeId();
            System.out.println(replacement_relationship.getStartNodeId() + "~~~" + replacement_relationship.getEndNodeId());

            int sp_tree_idx = this.dforests.get(i).findTreeIndex(r);
            SpanningTree sp_tree = this.dforests.get(i).trees.get(sp_tree_idx);


            TNode<RelationshipExt> min_node = sp_tree.findMinimum();

            //Find the sub euler tour from the first until before the given r
            SpanningTree left_sub_tree = new SpanningTree(sp_tree.neo4j, false);
            TNode<RelationshipExt> firstSplitor = sp_tree.findLeftSubTree(min_node, r, left_sub_tree);
            left_sub_tree.fixIfSingle(r);

            //Find the sub euler tour from the first given r to the second given r
            //if r == (v,w), first r means (v,w) or (w,v), the second r means the reverse of the first r, such as (w,v) or (v,w)
            SpanningTree middle_sub_tree = new SpanningTree(sp_tree.neo4j, false);
            TNode<RelationshipExt> secondSplitor = sp_tree.findMiddleSubTree(firstSplitor, r, middle_sub_tree);
            middle_sub_tree.fixIfSingle(r);


            //Find the sub euler tour from after the second r to the end of the Euler tour
            SpanningTree right_sub_tree = new SpanningTree(sp_tree.neo4j, false);
            sp_tree.findRightSubTree(secondSplitor, right_sub_tree);
            left_sub_tree.combineTree(right_sub_tree); //combine left tree and right, the cutting process of the euler tree.
            left_sub_tree.fixIfSingle(r);

            right_sub_tree = middle_sub_tree;
            left_sub_tree.fixIfSingle(r);
            right_sub_tree.fixIfSingle(r);

            if (left_sub_tree.N_nodes.contains(sid)) {
                System.out.println("re-root left sub-tree on node " + sid);
                left_sub_tree.reroot(sid, keyUpdatesMap, eid, i);
                System.out.println("re-root right sub-tree on node " + eid);
                right_sub_tree.reroot(eid, keyUpdatesMap, sid, i);
                connectTwoTree(left_sub_tree, right_sub_tree, replacement_relationship, keyUpdatesMap, i);
                System.out.println("************************************************");
//                left_sub_tree.rbtree.root.print();
            } else {
                System.out.println("re-root left sub-tree on node " + eid);
                left_sub_tree.reroot(eid, keyUpdatesMap, sid, i);
                System.out.println("re-root right sub-tree on node " + sid);
                right_sub_tree.reroot(sid, keyUpdatesMap, eid, i);
                connectTwoTree(left_sub_tree, right_sub_tree, replacement_relationship, keyUpdatesMap, i);
//                left_sub_tree.rbtree.root.print();
            }
            this.dforests.get(i).trees.remove(sp_tree_idx);
            this.dforests.get(i).trees.add(left_sub_tree);
        }
    }

    private void connectTwoTree(SpanningTree left_sub_tree, SpanningTree right_sub_tree, Relationship replacement_relationship, HashMap<Integer, Integer> keyUpdatesMap, int level) {
        System.out.println("connecting two tree");
        int src_id = (int) replacement_relationship.getStartNodeId();
        int dest_id = (int) replacement_relationship.getEndNodeId();


        int left_id = -1, right_id = -1;

        if (left_sub_tree.N_nodes.contains((long) src_id)) {
            System.out.println(src_id + " is in the left tree");
            left_id = src_id;
        } else if (right_sub_tree.N_nodes.contains((long) src_id)) {
            System.out.println(src_id + " is in the right tree");
            right_id = src_id;
        } else {
            System.out.println(src_id + " is not in neither the left nor the right tree");
        }


        if (left_sub_tree.N_nodes.contains((long) dest_id)) {
            System.out.println(dest_id + " is in the left tree");
            left_id = dest_id;
        } else if (right_sub_tree.N_nodes.contains((long) dest_id)) {
            System.out.println(dest_id + " is in the right tree");
            right_id = dest_id;
        } else {
            System.out.println(dest_id + " is not in neither the left nor the right tree");
        }

        if (left_id == -1 || right_id == -1) {
            System.err.println("Can not find src or dest neither in the left tree nor in the right tree");
            System.exit(0);
        }


        RelationshipExt rel_ext = new RelationshipExt(replacement_relationship, left_id, right_id);
        RelationshipExt reverse_rel_ext = new RelationshipExt(replacement_relationship, right_id, left_id);

        int max_key;
        max_key = left_sub_tree.findMaximumKeyValue();

//        left_sub_tree.rbtree.root.print();
//        right_sub_tree.rbtree.root.print();
        System.out.println(max_key);

        //insert first appears of replacement_relationship into the tree
        TNode<RelationshipExt> sNode = new TNode<>(++max_key, rel_ext);
        left_sub_tree.insert(sNode);
//        System.out.println("insert the node "+sNode.key+"  "+sNode.item);
        left_sub_tree.updateRelationshipRBPointer(sNode.item, sNode.key, -1, level);


        if (!right_sub_tree.isSingle) {
            TNode<RelationshipExt> min_node = right_sub_tree.findMinimum();
            TNode<RelationshipExt> node = new TNode<>(min_node);
            int org_key = node.key;

            if (level == 0) {
                node.key = ++max_key;
            } else {
                node.key = keyUpdatesMap.get(min_node.key);
            }

            left_sub_tree.insert(node);
            left_sub_tree.updateRelationshipRBPointer(node.item, node.key, org_key, level);
//            System.out.println("insert the min node of the right sub_tree "+node.key+"  "+node.item);


            TNode<RelationshipExt> suc_node = right_sub_tree.rbtree.successor(min_node);
            int k = 0;
            while (suc_node != nil) {
                node = new TNode<>(suc_node);
                org_key = node.key;

                if (level == 0) {
                    node.key = ++max_key;
                } else {
                    node.key = keyUpdatesMap.get(suc_node.key);
                }

                left_sub_tree.insert(node);
                left_sub_tree.updateRelationshipRBPointer(node.item, node.key, org_key, level);
                suc_node = right_sub_tree.rbtree.successor(suc_node);
//                System.out.println("insert the node of the right sub_tree "+node.key+"  "+node.item +" to left sub tree");
//                k++;
//                if(k==20) break;
            }


        }

        TNode<RelationshipExt> eNode = new TNode<>(++max_key, reverse_rel_ext);
        left_sub_tree.insert(eNode);
        left_sub_tree.updateRelationshipRBPointer(eNode.item, eNode.key, -1, level);

        right_sub_tree.rbtree.root = nil; //empty right_tree
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
            System.out.println("the sub tree is a single tree, do not need to push to higher level (level " + (level_r + 1) + ")");
            sub_tree.updateTreeEdgeLevel(level_r);
            return;
        }

        sub_tree.updateTreeEdgeLevel(level_r);
        int new_level = level_r + 1;

        if (dforests.containsKey(new_level)) {
            dforests.get(new_level).addNewTrees(sub_tree);
            sub_tree.rbtree.root.print();
        } else {
            SpanningForests sp = new SpanningForests(new_level);
            System.out.println("Create new level " + new_level + " forests");
            sp.trees.add(sub_tree);
            dforests.put(new_level, sp);
        }

        System.out.println("put new spanning tree to level " + new_level + " forests");
        System.out.println("Starting to merge the level " + new_level + " forest ............. ");
        dforests.get(new_level).merge(new_level);
        System.out.print("Finished merge the level " + new_level + " forest .............");
        System.out.println("There are  " + dforests.get(new_level).trees.size() + " trees in level " + new_level + " forest .............");

    }
}
