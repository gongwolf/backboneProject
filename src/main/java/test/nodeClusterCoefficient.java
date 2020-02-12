package test;

import Neo4jTools.Line;
import Neo4jTools.Neo4jDB;
import org.neo4j.graphdb.*;

import java.util.*;

public class nodeClusterCoefficient {
    public static void main(String args[]) {
        nodeClusterCoefficient n = new nodeClusterCoefficient();
        n.twohopCoefficient("sub_ny_USA_Level0");
    }

    private void onehopCoefficient(String dbname) {
        HashMap<Double, Integer> distribution = new HashMap<>();
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
                double coefficient = 1.0 * num_connected_neighbors / (neighbors.size() * (neighbors.size() - 1));
                if (coefficient == 1.0) {
                    System.out.println(node + "  " + coefficient + "    " + num_connected_neighbors + " " + neighbors + "   ");
                }

                if (distribution.containsKey(coefficient)) {
                    distribution.put(coefficient, distribution.get(coefficient) + 1);
                } else {
                    distribution.put(coefficient, 1);
                }
            }

            TreeMap<Double, Integer> sorted_distribution = new TreeMap<>(distribution);


            for (Map.Entry<Double, Integer> e : sorted_distribution.entrySet()) {
                System.out.println(String.format("%.4f", e.getKey()) + "   " + e.getValue());
            }

            tx.success();
        }
        neo4j.closeDB();
    }


    private void twohopCoefficient(String dbname) {
        HashMap<Double, Integer> distribution = new HashMap<>();
        Neo4jDB neo4j = new Neo4jDB(dbname);
        neo4j.startDB(true);
        GraphDatabaseService graphdb = neo4j.graphDB;

        try (Transaction tx = graphdb.beginTx()) {

            ResourceIterator<Node> nodes_iter = graphdb.getAllNodes().iterator();

            while (nodes_iter.hasNext()) {
                Node c_node = nodes_iter.next();

                Iterator<Relationship> rel_iter = c_node.getRelationships(Line.Linked, Direction.BOTH).iterator();
                HashSet<Node> neighbors = new HashSet<>();
                while (rel_iter.hasNext()) {
                    neighbors.add(rel_iter.next().getOtherNode(c_node));
                }


                HashSet<Node> second_neighbors = new HashSet<>();
                for (Node n_node : neighbors) {
                    Iterator<Relationship> sec_rel_iter = n_node.getRelationships(Line.Linked, Direction.BOTH).iterator();

                    while (sec_rel_iter.hasNext()) {
                        Node sec_nb = sec_rel_iter.next().getOtherNode(n_node);
                        if (!neighbors.contains(sec_nb) && sec_nb.getId() != c_node.getId()) {
                            second_neighbors.add(sec_nb);
                        }
                    }
                }


                int num_connected_neighbors = getNumberOfConnectedNeighbors(neighbors, neo4j);
                int num_connected_sec_neighbors = getNumberOfConnectedNeighbors(second_neighbors, neo4j);
                int num_connected_one_with_sec_neighbors = getNumberOfConnectedNeighbors(neighbors, second_neighbors, neo4j);


                double coefficient1 = 1.0 * num_connected_neighbors / (neighbors.size() * (neighbors.size() - 1));
                coefficient1 = Double.isNaN(coefficient1) ? -1 : coefficient1;
                double coefficient2 = 1.0 * num_connected_one_with_sec_neighbors / (neighbors.size() * (second_neighbors.size()));
                double coefficient3 = 1.0 * num_connected_sec_neighbors / (second_neighbors.size() * (second_neighbors.size() - 1));
                coefficient3 = Double.isNaN(coefficient3) ? -1 : coefficient3;
                if (coefficient1 != -1 && coefficient3 != -1)
                    System.out.println(c_node.getId() + "  " + c_node.getDegree(Direction.BOTH) + "  " + coefficient1 + "  " + num_connected_one_with_sec_neighbors + " "
                            + neighbors.size() + " " + second_neighbors.size() + " " + coefficient3 + "   ");


//                if (distribution.containsKey(coefficient)) {
//                    distribution.put(coefficient, distribution.get(coefficient) + 1);
//                } else {
//                    distribution.put(coefficient, 1);
//                }
            }

            TreeMap<Double, Integer> sorted_distribution = new TreeMap<>(distribution);


            for (Map.Entry<Double, Integer> e : sorted_distribution.entrySet()) {
                System.out.println(String.format("%.4f", e.getKey()) + "   " + e.getValue());
            }

            tx.success();
        }
        neo4j.closeDB();
    }

    private int getNumberOfConnectedNeighbors(HashSet<Node> neighbors, HashSet<Node> second_neighbors, Neo4jDB neo4j) {
        int connctions = 0;

        ArrayList<Node> n_list = new ArrayList<>(neighbors);
        HashSet<Long> sec_neighbors_noe_ids = new HashSet<>();
        second_neighbors.forEach(n -> sec_neighbors_noe_ids.add(n.getId()));

        for (int i = 0; i < n_list.size(); i++) {
            for (int j = i + 1; j < n_list.size(); j++) {
                Iterator<Relationship> i_rel_iter = n_list.get(i).getRelationships(Line.Linked, Direction.BOTH).iterator();
                Iterator<Relationship> j_rel_iter = n_list.get(j).getRelationships(Line.Linked, Direction.BOTH).iterator();

                HashSet<Node> i_n_nodes = new HashSet<>();
                HashSet<Node> j_n_nodes = new HashSet<>();

                while (i_rel_iter.hasNext()) {
                    i_n_nodes.add(i_rel_iter.next().getOtherNode(n_list.get(i)));
                }

                while (j_rel_iter.hasNext()) {
                    j_n_nodes.add(j_rel_iter.next().getOtherNode(n_list.get(j)));
                }

                for (Node i_n : i_n_nodes) {
                    for (Node j_n : j_n_nodes) {
                        if (i_n.getId() == j_n.getId() && sec_neighbors_noe_ids.contains(i_n.getId())) {
                            connctions++;
                        }
                    }
                }

            }
        }


//        for (Node s_n : neighbors) {
//            Iterator<Relationship> rel_iter = s_n.getRelationships(Line.Linked, Direction.BOTH).iterator();
//            while (rel_iter.hasNext()) {
//                Node other_node = rel_iter.next().getOtherNode(s_n);
//                if (second_neighbors.contains(other_node)) {
//                    connctions++;
//                }
//            }
//        }
        return connctions;
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
