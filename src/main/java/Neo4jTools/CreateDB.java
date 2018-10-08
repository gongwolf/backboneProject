package Neo4jTools;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class CreateDB {
    private final String home_folder = System.getProperty("user.home");


    public static void main(String args[]) {
        CreateDB c = new CreateDB();
        c.createBusLineDataBase(1000, 7);
    }

    public void createBusLineDataBase(int graph_size, double samnode_t) {
        String sub_db_name = graph_size + "-" + samnode_t + "-" + "Lvevl0";
        Neo4jDB neo4j = new Neo4jDB(sub_db_name);
        neo4j.deleleDB();
        System.out.println("====================================================================");
        neo4j.startDB();
        System.out.println("====================================================================");
        String nodeFilePath = home_folder + "/mydata/projectData/BackBone/busline_" + graph_size + "_" + samnode_t + "/data/NodeInfo.txt";
        String EdgeFilePath = home_folder + "/mydata/projectData/BackBone/busline_" + graph_size + "_" + samnode_t + "/data/SegInfo.txt";
        System.out.println("node file path :" + nodeFilePath);
        System.out.println("edge file path :" + EdgeFilePath);

        GraphDatabaseService graphdb = neo4j.graphDB;

        int num_node = 0, num_edge = 0;

        try (Transaction tx = graphdb.beginTx()) {
            BufferedReader br = new BufferedReader(new FileReader(nodeFilePath));
            String line = null;
            while ((line = br.readLine()) != null) {
                //System.out.println(line);
                String[] attrs = line.split(" ");

                String id = attrs[0];
                double lat = Double.parseDouble(attrs[1]);
                double log = Double.parseDouble(attrs[2]);
                Node n = createNode(id, lat, log,graphdb);
                num_node++;
                if (num_node % 10000 == 0) {
                    System.out.println(num_node + " nodes was created");
                }
            }
            tx.success();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(EdgeFilePath));
            String line = null;

            ArrayList<String> ss = new ArrayList<>();

            while ((line = br.readLine()) != null) {
                ss.add(line);
                num_edge++;

                if (num_edge % 100000 == 0) {
                    process_batch_edges(ss,graphdb);
                    ss.clear();
                    System.out.println(num_edge+" edges were created");
                }
            }
            process_batch_edges(ss, graphdb);
            ss.clear();
            System.out.println(num_edge+" edges were created");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("there are total " + num_node + " nodes and " + num_edge + " edges");
        System.out.println("====================================================================");
        neo4j.closeDB();

    }

    private Node createNode(String id, double lat, double log, GraphDatabaseService graphdb) {
        Node n = graphdb.createNode(BNode.BusNode);
        n.setProperty("name", id);
        n.setProperty("lat", lat);
        n.setProperty("log", log);
        if (n.getId() != Long.valueOf(id)) {
            System.out.println("id not match  " + n.getId() + "->" + id);
        }
        return n;
    }

    private void process_batch_edges(ArrayList<String> ss, GraphDatabaseService graphdb) {
        try (Transaction tx = graphdb.beginTx()) {
            for (String line : ss) {
                String attrs[] = line.split(" ");
                String src = attrs[0];
                String des = attrs[1];
                double EDistence = Double.parseDouble(attrs[2]);
                double MetersDistance = Double.parseDouble(attrs[3]);
                double RunningTime = Double.parseDouble(attrs[4]);
                createRelation(src, des, EDistence, MetersDistance, RunningTime,graphdb);
            }
            tx.success();
        }
    }

    private void createRelation(String src, String des, double eDistence, double metersDistance, double runningTime,GraphDatabaseService graphdb) {
        try {
            Node srcNode = graphdb.getNodeById(Long.valueOf(src));
            Node desNode = graphdb.getNodeById(Long.valueOf(des));

            Relationship rel = srcNode.createRelationshipTo(desNode, Line.Linked);
            rel.setProperty("EDistence", eDistence);
            rel.setProperty("MetersDistance", metersDistance);
            rel.setProperty("RunningTime", runningTime);
        } catch (Exception e) {
            System.out.println(src + "-->" + des);
            e.printStackTrace();
            System.exit(0);
        }
    }
}
