package v5LinkedList.clusterversion;

import org.neo4j.graphdb.Node;

public class myNode {
    Node node;
    double coefficient;
    boolean visited = false;
    boolean inqueue = false;

    public myNode(Node node, double coefficient) {
        this.node = node;
        this.coefficient = coefficient;
    }

    @Override
    public String toString() {
        return "myNode{" +
                "node=" + node +
                ", coefficient=" + coefficient +
                ", visited=" + visited +
                ", inqueue=" + inqueue +
                '}';
    }
}
