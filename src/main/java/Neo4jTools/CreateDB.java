package Neo4jTools;

import javafx.util.Pair;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class CreateDB {
    private final String home_folder = System.getProperty("user.home");


    public static void main(String args[]) {

        int graphsize = 10000;
        int degree = 4;
        int dimension = 3;

        CreateDB c = new CreateDB();
        c.createRandomGraph(graphsize, degree, dimension);
//        c.createBusLineDataBase(100 , 28);
    }

    private void createRandomGraph(int graphsize, int degree, int dimension) {
        String sub_db_name = graphsize + "_" + degree + "_" + dimension + "_Level0";
        Neo4jDB neo4j = new Neo4jDB(sub_db_name);
        neo4j.deleleDB();
        System.out.println("====================================================================");
        neo4j.startDB(false);
        System.out.println(neo4j.DB_PATH);
        System.out.println("====================================================================");
        String nodeFilePath = home_folder + "/mydata/projectData/BackBone/testRandomGraph_" + graphsize + "_" + degree + "_" + dimension + "/data/NodeInfo.txt";
        String EdgeFilePath = home_folder + "/mydata/projectData/BackBone/testRandomGraph_" + graphsize + "_" + degree + "_" + dimension + "/data/SegInfo.txt";
//        String nodeFilePath = home_folder + "/mydata/projectData/BackBone/busline_14_0.0/data/NodeInfo.txt";
//        String EdgeFilePath = home_folder + "/mydata/projectData/BackBone/busline_14_0.0/data/SegInfo.txt";

        System.out.println("node file path :" + nodeFilePath);
        System.out.println("edge file path :" + EdgeFilePath);
        GraphDatabaseService graphdb = neo4j.graphDB;
        int num_node = 0, num_edge = 0;

        try (Transaction tx = graphdb.beginTx()) {
            BufferedReader br = new BufferedReader(new FileReader(nodeFilePath));
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] attrs = line.split(" ");

                String id = attrs[0];
                double lat = Double.parseDouble(attrs[1]);
                double log = Double.parseDouble(attrs[2]);
                Node n = createNode(id, lat, log, graphdb);
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


        HashMap<Pair<Integer, Integer>, double[]> edges = new HashMap<>();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(EdgeFilePath));
            String line = null;
            while ((line = br.readLine()) != null) {
                num_edge++;
                int sid = Integer.parseInt(line.split(" ")[0]);
                int did = Integer.parseInt(line.split(" ")[1]);
                double c1 = Double.parseDouble(line.split(" ")[2]);
                double c2 = Double.parseDouble(line.split(" ")[3]);
                double c3 = Double.parseDouble(line.split(" ")[4]);
                double[] costs = new double[]{c1, c2, c3};
                Pair<Integer, Integer> relations = new Pair<>(sid, did);

                //Treat the graph as an in-directional graph
                if (!existedEdges(relations, edges)) {
                    edges.put(relations, costs);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("number of total edges:" + num_edge + ", number of undirected edges:" + edges.size());

        int idx_i = 0;
        ArrayList<String> ss = new ArrayList<>();
        for (Map.Entry<Pair<Integer, Integer>, double[]> e : edges.entrySet()) {
            idx_i++;
            StringBuilder str = new StringBuilder();
            str.append(e.getKey().getKey()).append(" "); //sid
            str.append(e.getKey().getValue()).append(" "); //did
            str.append(e.getValue()[0]).append(" "); //c1
            str.append(e.getValue()[1]).append(" "); //c2
            str.append(e.getValue()[2]); //c3
            ss.add(str.toString());
            if (idx_i % 10000 == 0) {
                process_batch_edges(ss, graphdb);
                ss.clear();
                System.out.println(idx_i + " edges were created");
            }
        }
        process_batch_edges(ss, graphdb);
        ss.clear();
        System.out.println(idx_i + " edges were created");

        System.out.println("there are total " + num_node + " nodes and " + num_edge + " edges" + " and undirected edges " + idx_i);
        System.out.println("====================================================================");
        neo4j.closeDB();

    }

    public void createBusLineDataBase(int graph_size, double samnode_t) {
        String sub_db_name = graph_size + "_" + samnode_t + "_" + "Level0";
        Neo4jDB neo4j = new Neo4jDB(sub_db_name);
        neo4j.deleleDB();
        System.out.println("====================================================================");
        neo4j.startDB(false);
        System.out.println(neo4j.DB_PATH);
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
                Node n = createNode(id, lat, log, graphdb);
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


        HashMap<Pair<Integer, Integer>, double[]> edges = new HashMap<>();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(EdgeFilePath));
            String line = null;


            while ((line = br.readLine()) != null) {
                num_edge++;
                int sid = Integer.parseInt(line.split(" ")[0]);
                int did = Integer.parseInt(line.split(" ")[1]);
                double c1 = Double.parseDouble(line.split(" ")[2]);
                double c2 = Double.parseDouble(line.split(" ")[3]);
                double c3 = Double.parseDouble(line.split(" ")[4]);
                double[] costs = new double[]{c1, c2, c3};
                Pair<Integer, Integer> relations = new Pair<>(sid, did);
                if (!existedEdges(relations, edges)) {
                    edges.put(relations, costs);

                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("number of total edges:" + num_edge + ", number of undirected edges:" + edges.size());

        int idx_i = 0;
        ArrayList<String> ss = new ArrayList<>();
        for (Map.Entry<Pair<Integer, Integer>, double[]> e : edges.entrySet()) {
            idx_i++;
            StringBuilder str = new StringBuilder();
            str.append(e.getKey().getKey()).append(" "); //sid
            str.append(e.getKey().getValue()).append(" "); //did
            str.append(e.getValue()[0]).append(" "); //c1
            str.append(e.getValue()[1]).append(" "); //c2
            str.append(e.getValue()[2]); //c3
            ss.add(str.toString());
            if (idx_i % 100000 == 0) {
                process_batch_edges(ss, graphdb);
                ss.clear();
                System.out.println(idx_i + " edges were created");
            }
        }
        process_batch_edges(ss, graphdb);
        ss.clear();
        System.out.println(idx_i + " edges were created");

        System.out.println("there are total " + num_node + " nodes and " + num_edge + " edges" + " and undirected edges " + idx_i);
        System.out.println("====================================================================");
        neo4j.closeDB();

    }

    private boolean existedEdges(Pair<Integer, Integer> relations, HashMap<Pair<Integer, Integer>, double[]> edges) {

        int sid = relations.getKey();
        int did = relations.getValue();
        Pair<Integer,Integer> reverse_rel = new Pair<>(did,sid);
        return edges.containsKey(reverse_rel) || edges.containsKey(relations);

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
                createRelation(src, des, EDistence, MetersDistance, RunningTime, graphdb);
            }
            tx.success();
        }
    }

    private void createRelation(String src, String des, double eDistence, double metersDistance, double runningTime, GraphDatabaseService graphdb) {
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
