package v5LinkedList;

import Neo4jTools.Neo4jDB;
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
                return current_tree;
            }
        }
        return null;
    }

    public SpanningTree findTree(int rid) {
        for (SpanningTree current_tree : trees) {
            if (current_tree.hasEdge(rid)) {
                return current_tree;
            }
        }
        return null;
    }

    public int findTreeIndex(Relationship r) {
        for (int index = 0; index < trees.size(); index++) {
            if (trees.get(index).hasEdge(r)) {
                return index;
            }
        }
        return -1;
    }

    public int findTreeIndex(long rel_id) {
        for (int index = 0; index < trees.size(); index++) {
            if (trees.get(index).hasEdge(rel_id)) {
                return index;
            }
        }
        return -1;
    }

    /**
     * because i is always less than j, i is deleted before j.
     * After deletion of tree i, the index of tree j needs to decrease 1.
     * **/
    public boolean putNewSpanningTree(SpanningTree sub_tree) {
        this.trees.add(sub_tree);
        if (trees.size() == 1) {
            return true;
        }

        Pair<Integer, Integer> tree_idx;
        while ((tree_idx = hasCouldMergedTree()) != null) {
            int i = tree_idx.getKey();
            int j = tree_idx.getValue();

            SpanningTree new_tree = mergeTree(trees.get(i), trees.get(j));
            trees.remove(i);
            trees.remove(j - 1);
            trees.add(new_tree);
        }
        return true;
    }

    private SpanningTree mergeTree(SpanningTree onetree, SpanningTree anothertree) {
        SpanningTree new_tree = new SpanningTree(onetree.neo4j, false);
        new_tree.N_nodes.addAll(onetree.N_nodes);
        new_tree.N_nodes.addAll(anothertree.N_nodes);
        new_tree.SpTree.addAll(onetree.SpTree);
        new_tree.SpTree.addAll(anothertree.SpTree);
        new_tree.N = new_tree.N_nodes.size();
        new_tree.E = new_tree.SpTree.size();
        new_tree.FindEulerTourStringWiki();
        return new_tree;

    }

    public Pair<Integer, Integer> hasCouldMergedTree() {
        for (int i = 0; i < trees.size(); i++) {
            for (int j = i + 1; j < trees.size(); j++) {
                if (hasCommonNodes(trees.get(i), trees.get(j))) {
                    return new Pair<>(i, j);
                }
            }
        }
        return null;
    }

    public boolean hasCommonNodes(SpanningTree spanningTree, SpanningTree spanningTree1) {
        HashSet<Long> nodes_1 = spanningTree.N_nodes;
        HashSet<Long> nodes_2 = spanningTree1.N_nodes;
        for (long nid1 : nodes_1) {
            if (nodes_2.contains(nid1)) {
                return true;
            }
        }
        return false;
    }


    public boolean pushEdgeToHigherLevelForest(Relationship next_level_rel, Neo4jDB neo4j) {
        SpanningTree new_sub_tree = new SpanningTree(neo4j, false);
        new_sub_tree.initializedSingleEdge(next_level_rel);
        this.trees.add(new_sub_tree);

        if (trees.size() == 1) {
            return true;
        }

        Pair<Integer, Integer> tree_idx;
        while ((tree_idx = hasCouldMergedTree()) != null) {
            int i = tree_idx.getKey();
            int j = tree_idx.getValue();

            SpanningTree new_tree = mergeTree(trees.get(i), trees.get(j));
            /**
             * because i is always less than j, i is deleted before j.
             * After deletion of tree i, the index of tree j needs to decrease 1.
             * **/
            trees.remove(i);
            trees.remove(j - 1);

            trees.add(new_tree);
        }
        return true;
    }

    public boolean containsEdge(long id) {
        for (SpanningTree t : trees) {
            if (t.SpTree.contains(id)) {
                return true;
            }
        }
        return false;
    }


}
