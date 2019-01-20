package v3;

import javafx.util.Pair;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.HashSet;

public class SpanningForests {
    public ArrayList<SpanningTree> trees = new ArrayList<>();
    public int level;

    public SpanningForests(int level) {
        this.level = level;
    }

    public SpanningTree findTree(Relationship r) {
        for (SpanningTree current_tree : trees) {
            if (current_tree.hasEdge(r)) {
//                System.out.println("find spanning tree whose contains the edge " + r);
                return current_tree;
            }
        }
        return null;
    }


    public int findTreeIndex(Relationship r) {
        for (int i = 0; i < trees.size(); i++) {
            SpanningTree current_tree = trees.get(i);
            if (current_tree.hasEdge(r)) {
                System.out.println("the index of the spanning tree whose contains the edge " + r + " is " + i);
                return i;
            }
        }
        return -1;
    }


    public void addNewTrees(SpanningTree sptree) {
        trees.add(sptree);
    }

    public boolean merge(int level_r) {
        if (trees.size() == 1) {
            return true;
        }

        Pair<Integer, Integer> tree_idx;
        while ((tree_idx = hasCouldMergedTree()) != null) {
            int i = tree_idx.getKey();
            int j = tree_idx.getValue();
            SpanningTree new_tree = mergeTree(trees.get(i), trees.get(j),level_r);
            /**
             * because i is always less than j, i is deleted before j.
             * After deletion of tree i, the index of tree j needs to decrease 1.
             * **/
            trees.remove(i);

            trees.remove(j - 1);
            trees.add(new_tree);

        }
        return false;
    }

    private SpanningTree mergeTree(SpanningTree spanningTree, SpanningTree spanningTree1, int level_r) {
        SpanningTree new_tree = new SpanningTree();
        new_tree.N_nodes.addAll(spanningTree.N_nodes);
        new_tree.N_nodes.addAll(spanningTree1.N_nodes);

        new_tree.SpTree.addAll(spanningTree.SpTree);
        new_tree.SpTree.addAll(spanningTree1.SpTree);

        new_tree.N = new_tree.N_nodes.size();
        new_tree.FindAdjList();
        new_tree.EulerTourString();

        return new_tree;

    }

    private Pair<Integer, Integer> hasCouldMergedTree() {
        for (int i = 0; i < trees.size(); i++) {
            for (int j = i + 1; j < trees.size(); j++) {
                if (hasCommonNodes(trees.get(i), trees.get(j))) {
                    return new Pair<>(i, j);
                }
            }
        }
        return null;
    }

    private boolean hasCommonNodes(SpanningTree spanningTree, SpanningTree spanningTree1) {
        HashSet<Long> nodes_1 = spanningTree.N_nodes;
        HashSet<Long> nodes_2 = spanningTree1.N_nodes;
        for (long nid1 : nodes_1) {
            if (nodes_2.contains(nid1)) {
                return true;
            }
        }
        return false;
    }

    public void findTrees(Relationship replacement_relationship, SpanningTree left_sub_tree, SpanningTree right_sub_tree) {
        boolean findLeft = false, findRight = false;
        for (SpanningTree t : trees) {
            if (findLeft && findRight) {
                return;
            }
            if (t.N_nodes.contains(replacement_relationship.getStartNodeId())) {
                left_sub_tree = t;
                findLeft = true;
            } else if (t.N_nodes.contains(replacement_relationship.getEndNodeId())) {
                right_sub_tree = t;
                findRight = true;
            }
        }
    }
}
