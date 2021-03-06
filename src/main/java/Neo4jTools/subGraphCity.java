package Neo4jTools;

import javafx.util.Pair;
import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.*;

import java.io.*;
import java.util.*;

public class subGraphCity {
    int numberofK = 100;
    String sub_db_name = "ny_USA_Level0";
    String base_dir = "/home/gqxwolf/mydata/projectData/BackBone/busline_sub_graph_NY/sub_graph/" + numberofK + "K/";
    String EdgesPath = base_dir + "sub_level0_ny_SegInfo_" + numberofK + "K.txt";
    String NodePath = base_dir + "/sub_level0_ny_NodeInfo_" + numberofK + "K.txt";
    String temp_resource_folder = "/home/gqxwolf/mydata/projectData/BackBone/busline_sub_graph_NY_" + numberofK + "K/";

    public static void main(String args[]) {
        subGraphCity sgc = new subGraphCity();
        sgc.selectSubGraph();
        System.out.println("========================================================================================================");
    }

    private void selectSubGraph() {

        try {
            if (!new File(this.base_dir).exists()) {
                File sub_graph_folder = new File(this.base_dir);
                File temp_resource_folder = new File(this.temp_resource_folder);
                FileUtils.forceMkdir(sub_graph_folder);
                FileUtils.forceMkdir(temp_resource_folder);
                System.out.println("Create the new Folder " + sub_graph_folder);
                System.out.println("Create the new Folder " + temp_resource_folder);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        Neo4jDB neo4j = new Neo4jDB(sub_db_name);
        System.out.println("====================================================================");
        neo4j.startDB(true);
        System.out.println(neo4j.DB_PATH);
        System.out.println("number of nodes :" + neo4j.getNumberofNodes());
        System.out.println("number of edges :" + neo4j.getNumberofEdges());
        System.out.println("====================================================================");

        HashSet<Relationship> edges = new HashSet<>();
        HashSet<Long> nodes = new HashSet<>();

        int target_graph_size = numberofK * 1000;

        try (Transaction tx = neo4j.graphDB.beginTx()) {

            ArrayList<Node> nodelist = new ArrayList<>();
            ResourceIterable<Node> nodes_iterable = neo4j.graphDB.getAllNodes();
            ResourceIterator<Node> nodes_iter = nodes_iterable.iterator();
            while (nodes_iter.hasNext()) {
                Node node = nodes_iter.next();
                nodelist.add(node);
            }
            tx.success();

            Node first_node = getRondomNodes(nodelist);

            Queue<Node> q = new LinkedList<>();
            q.add(first_node);

            System.out.println("Start node ------> [" + first_node + "]  to Run BFS graph traverse");
            nodes.add(first_node.getId());
            while (!q.isEmpty()) {
                Node n = q.poll();
                for (Relationship rel : n.getRelationships(Direction.BOTH)) {
                    Node other_n = rel.getOtherNode(n);

                    if (nodes.size() < target_graph_size && !nodes.contains(other_n.getId())) {
                        nodes.add(other_n.getId());
                        q.add(other_n);
                    }

                    if (nodes.contains(rel.getStartNodeId()) && nodes.contains(rel.getEndNodeId())) {
                        edges.add(rel);
                    }
                }
            }


            System.out.println(edges.size());
            System.out.println(nodes.size());

            writeToDisk(nodes, edges, neo4j);
            tx.success();
        }

//        System.out.println(visited_map.size());
        neo4j.closeDB();

    }

    private void writeToDisk(HashSet<Long> nodes, HashSet<Relationship> edges, Neo4jDB neo4j) {
        File e_file = new File(this.EdgesPath);
        File n_file = new File(this.NodePath);

        if (e_file.exists()) {
            e_file.delete();
        }

        if (n_file.exists()) {
            n_file.delete();
        }

        HashMap<Long, Long> node_id_mapping = new HashMap<>();

        try (FileWriter fw = new FileWriter(NodePath, true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw);
             Transaction tx = neo4j.graphDB.beginTx()) {

            System.out.println(NodePath);
            TreeSet<Long> tm = new TreeSet<>(new LongComparator());
            tm.addAll(nodes);
            long mapped_id = 0;
            for (long node_id : tm) {

                node_id_mapping.put(node_id, mapped_id++);

                StringBuffer sb = new StringBuffer();
                Node node = neo4j.graphDB.getNodeById(node_id);
                sb.append(node_id_mapping.get(node_id)).append(" ");
                sb.append(node.getProperty("lat")).append(" ");
                sb.append(node.getProperty("log")).append(" ");
                out.println(sb.toString().trim());
            }
            tx.success();
        } catch (IOException e) {
            e.printStackTrace();
        }


        try (FileWriter fw = new FileWriter(EdgesPath, true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw);
             Transaction tx = neo4j.graphDB.beginTx()) {
            System.out.println(EdgesPath);

            for (Relationship e : edges) {
                StringBuffer sb = new StringBuffer();
                long snodeId = node_id_mapping.get(e.getStartNodeId());
                long enodeId = node_id_mapping.get(e.getEndNodeId());
                sb.append(snodeId).append(" ");
                sb.append(enodeId).append(" ");
                for (String p : neo4j.propertiesName) {
                    double cost = (double) e.getProperty(p);
                    sb.append(cost).append(" ");
                }
                out.println(sb.toString().trim());
            }

            tx.success();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Node getRondomNodes(ArrayList<Node> nodelist) {
        Random r = new Random();
        int idx = r.nextInt(nodelist.size());
        return nodelist.get(idx);
    }
}
