package Neo4jTools.tools;

import Neo4jTools.Neo4jDB;
import org.neo4j.graphdb.*;

import java.util.HashMap;
import java.util.TreeMap;

public class GraphProperties {
    public static void main(String args[]) {
        GraphProperties gp = new GraphProperties();
        gp.degreeDistribution("sub_ny_USA_Level0");
        gp.degreeDistribution("sub_ny_USA_20K_Level0");
        gp.degreeDistribution("sub_ny_USA_30K_Level0");
        gp.degreeDistribution("sub_ny_USA_40K_Level0");
//        gp.degreeDistribution("sub_ny_USA_50K_Level0");
        gp.degreeDistribution("sub_ny_USA_100K_Level0");
    }

    public void degreeDistribution(String db_name) {
        HashMap<Integer, Integer> degreedistribution = new HashMap<>();
        Neo4jDB neo4j = new Neo4jDB(db_name);
        neo4j.startDB(true);
        GraphDatabaseService graphdb = neo4j.graphDB;
        try (Transaction tx = graphdb.beginTx()) {
            ResourceIterator<Node> nodes_iter = graphdb.getAllNodes().iterator();
            while (nodes_iter.hasNext()) {
                Node n = nodes_iter.next();
                int degree = n.getDegree();

                if (degreedistribution.containsKey(degree)) {
                    degreedistribution.put(degree, degreedistribution.get(degree) + 1);
                } else {
                    degreedistribution.put(degree, 1);
                }
            }
            tx.success();
        }
        TreeMap<Integer, Integer> sorted_degree_distribution = new TreeMap<>(degreedistribution);

        System.out.println(graphdb);
        sorted_degree_distribution.forEach((k, v) -> System.out.println("degree:" + k + "  number:" + v));
        neo4j.closeDB();
    }

}
