package v5LinkedList;

import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;

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


    public void putNewSpanningTree(SpanningTree middle_sub_tree) {
    }
}
