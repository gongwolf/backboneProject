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

}
