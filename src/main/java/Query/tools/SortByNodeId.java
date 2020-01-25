package Query.tools;

import org.neo4j.graphdb.Node;

import java.util.Comparator;

public class SortByNodeId implements Comparator<Node> {

    @Override
    public int compare(Node o1, Node o2) {
        return (int) (o1.getId() - o2.getId());
    }
}
