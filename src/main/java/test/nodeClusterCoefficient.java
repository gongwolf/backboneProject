package test;

import Neo4jTools.Line;
import Neo4jTools.Neo4jDB;
import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

public class nodeClusterCoefficient {
    public static void main(String args[]) {
        nodeClusterCoefficient n = new nodeClusterCoefficient();
        n.onehopCoefficient("sub_ny_USA_Level0");
    }

    private void onehopCoefficient(String dbname) {
        Neo4jDB neo4j = new Neo4jDB(dbname);
        neo4j.startDB(true);
        GraphDatabaseService graphdb = neo4j.graphDB;

        try (Transaction tx = graphdb.beginTx()) {

            ResourceIterator<Node> nodes_iter = graphdb.getAllNodes().iterator();

            while (nodes_iter.hasNext()) {
                Node node = nodes_iter.next();

                Iterator<Relationship> rel_iter = node.getRelationships(Line.Linked, Direction.BOTH).iterator();
                HashSet<Node> neighbors = new HashSet<>();
                while (rel_iter.hasNext()) {
                    neighbors.add(rel_iter.next().getOtherNode(node));
                }

                int num_connected_neighbors = getNumberOfConnectedNeighbors(neighbors, neo4j);
                System.out.println(node + "  " + (1.0 * num_connected_neighbors / (neighbors.size() * (neighbors.size() - 1))) +"    " + num_connected_neighbors + " " + neighbors + "   " );

            }

            tx.success();
        }
        neo4j.closeDB();
    }

    private int getNumberOfConnectedNeighbors(HashSet<Node> neighbors, Neo4jDB neo4j) {
        int connctions = 0;
        for (Node s_n : neighbors) {
            Iterator<Relationship> rel_iter = s_n.getRelationships(Line.Linked, Direction.BOTH).iterator();
            while (rel_iter.hasNext()) {
                Node other_node = rel_iter.next().getOtherNode(s_n);
                if (neighbors.contains(other_node)) {
                    connctions++;
                }
            }
        }
        return connctions;
    }
}
