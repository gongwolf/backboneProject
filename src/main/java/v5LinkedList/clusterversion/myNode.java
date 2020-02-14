package v5LinkedList.clusterversion;

import org.neo4j.graphdb.Node;

public class myNode {
    public long id;
//    Node node;
    double coefficient;
    boolean visited = false;
    boolean inqueue = false;

    public myNode(long node_id, Node node, double coefficient) {
        this.id = node_id;
//        this.node = node;
        this.coefficient = coefficient;
    }

    @Override
    public String toString() {
        return "myNode{" +
//                "node=" + node +
                ", coefficient=" + coefficient +
                ", visited=" + visited +
                ", inqueue=" + inqueue +
                '}';
    }
}
