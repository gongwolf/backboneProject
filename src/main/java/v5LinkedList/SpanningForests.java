package v5LinkedList;

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

    public int findTreeIndex(Relationship r) {
        for (int index = 0; index < trees.size(); index++) {
            if (trees.get(index).hasEdge(r)) {
                return index;
            }
        }
        return -1;
    }


    public boolean putNewSpanningTree(SpanningTree sub_tree) {

        this.trees.add(sub_tree);

        if (trees.size() == 1) {
            System.out.println("There is only one tree in level " + level + ", do not need to merge");
            return true;
        }

        Pair<Integer, Integer> tree_idx;
        while ((tree_idx = hasCouldMergedTree()) != null) {
            int i = tree_idx.getKey();
            int j = tree_idx.getValue();
            System.out.println("Merging tree ..... " + i + "  and  " + j);

            SpanningTree new_tree = mergeTree(trees.get(i), trees.get(j));
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
}
