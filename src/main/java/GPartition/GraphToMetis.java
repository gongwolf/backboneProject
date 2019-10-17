package GPartition;

import javafx.util.Pair;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class GraphToMetis {
    String home_folder = System.getProperty("user.home");
    String graph_info_folder = home_folder + "/mydata/projectData/BackBone/testRandomGraph_10000_4_3/data";
    String init_edges_file = graph_info_folder + "/SegInfo.txt";
    String init_nodes_file = graph_info_folder + "/NodeInfo.txt";
    String init_target_path = this.graph_info_folder + "/metis_formation.graph";
    String partition_logs = this.graph_info_folder + "/partion_logs.txt";

    int graphsize;
    int degree;
    int dimension;
    int weight_index;// the index of the costs that is treated as the weight of edges, which is used to metis partition


    long number_of_nodes = 0;
    long number_of_edges = 0;
    String fmt = "001";
    String nonc = null;

//    HashMap<Pair<Long, Long>, double[]> edges = new HashMap<Pair<Long, Long>, double[]>();
//    HashMap<Long, HashMap<Long, Double>> gp_metis_formation = new HashMap<>();  // the node id -->> <neighborID, cost>

    public GraphToMetis(int weight_index) {
        this.graphsize = 10000;
        this.degree = 4;
        this.dimension = 3;
        this.graph_info_folder = home_folder + "/mydata/projectData/BackBone/testRandomGraph_" + graphsize + "_" + degree + "_" + dimension + "/data";
        this.weight_index = weight_index;
    }

    public GraphToMetis(int graphsize, int degree, int dimension, int weight_index) {
        this.graphsize = graphsize;
        this.degree = degree;
        this.dimension = dimension;
        this.weight_index = weight_index;
        this.graph_info_folder = home_folder + "/mydata/projectData/BackBone/testRandomGraph_" + graphsize + "_" + degree + "_" + dimension + "/data";
    }

    public void MultiLevelGraphPartition(int fan, int nodes_in_leaf) {
        int level = 0;

        int max_number_vertex_in_nodes = this.graphsize;

        while(max_number_vertex_in_nodes>= nodes_in_leaf);

    }

    public static void main(String args[]) {
        int weight_index = 0;

        GraphToMetis gTom = new GraphToMetis(weight_index);
//        gTom.MultiLevelGraphPartition(2, 30);

//        HashMap<Pair<Long, Long>, double[]> edges = new HashMap<>();
//        HashMap<Long, HashMap<Long, Double>> gp_metis_formation = new HashMap<>();  // the node id -->> <neighborID, cost>
//        gTom.readTheNodesFromDisk(gp_metis_formation, gTom.init_nodes_file);
//        gTom.readTheEdgesFromDisk(gp_metis_formation,gTom.init_edges_file,edges);
//        System.out.println("there are " + gTom.number_of_edges + " edges");
//        gTom.writeToDisk(gp_metis_formation, gTom.init_target_path);
//        System.out.println("The graph is saved to : "+gTom.init_target_path);
//        gTom.callGPMetisCommand(true, 100, gTom.init_target_path);
    }

    private void callGPMetisCommand(boolean needLogs, int partition_num, String graph_path) {
        String command = "gpmetis -objtype=vol " + init_target_path + " " + partition_num;
        String ls_command = "ls " + graph_info_folder;
        ProcessBuilder pb = new ProcessBuilder("bash", "-c", command);
        try {
            pb.directory(new File(graph_info_folder));
            Process process = pb.start();
            System.out.println(pb.directory());

            if (needLogs) {

                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
                LocalDateTime now = LocalDateTime.now();

                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(process.getInputStream()));
                String line;
                FileWriter fw = new FileWriter(partition_logs, true);
                BufferedWriter bw = new BufferedWriter(fw);

                bw.write("=========================================================================================================================\n");
                bw.write(dtf.format(now) + "\n");
                bw.write("Run the command " + StringUtils.join(pb.command()) + "\n\n\n");

                while ((line = reader.readLine()) != null) {
                    bw.write(line + "\n");
                }
                bw.write("=========================================================================================================================\n\n\n\n");

                bw.close();
                fw.close();
            }


            int exitVal = process.waitFor();
            if (exitVal == 0) {
                System.out.println("Success!");
            } else {
                System.out.println("failure !!!!");
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void writeToDisk(HashMap<Long, HashMap<Long, Double>> gp_metis_formation, String target_path) {
        this.number_of_nodes = gp_metis_formation.size();

        File target_file = new File(target_path);
        if (target_file.exists()) {
            target_file.delete();
        }

        int count = 1;

        try (FileWriter fw = new FileWriter(target_path);
             BufferedWriter bw = new BufferedWriter(fw)) {
            bw.write(number_of_nodes + " " + number_of_edges + " " + this.fmt + " \n");

            for (Map.Entry<Long, HashMap<Long, Double>> e : gp_metis_formation.entrySet()) {
//                System.out.println(e.getKey());
                StringBuffer sb = new StringBuffer();
                for (Map.Entry<Long, Double> ne : e.getValue().entrySet()) {
                    sb.append(ne.getKey()).append(" ").append(ne.getValue().intValue()).append(" ");
                }
                String line = sb.toString().trim() + "\n";
//                System.out.print(line);
                bw.write(line);
//                if(count++%10==0){
//                    break;
//                }

            }


        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void readTheEdgesFromDisk(HashMap<Long, HashMap<Long, Double>> gp_metis_formation, String edges_file, HashMap<Pair<Long, Long>, double[]> edges) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(edges_file));
            String line = null;

            while ((line = br.readLine()) != null) {
//                System.out.println(line);
                String[] edge_info = line.split(" ");
                long start_id = Long.parseLong(edge_info[0]) + 1; // metis node id start from 1
                long end_id = Long.parseLong(edge_info[1]) + 1; // metis node is start from 1

                double[] costs = new double[this.dimension];
                costs[0] = Double.parseDouble(edge_info[2]);
                costs[1] = Double.parseDouble(edge_info[3]);
                costs[2] = Double.parseDouble(edge_info[4]);

                Pair<Long, Long> relations = new Pair<>(start_id, end_id);
                if (!existedEdges(relations, edges)) {
                    edges.put(relations, costs);
                }

                //deal with the edge start_id --> end_id
                HashMap<Long, Double> neighbor_information;
                if (gp_metis_formation.get(start_id) != null) {
                    neighbor_information = gp_metis_formation.get(start_id);
                } else {
                    neighbor_information = new HashMap<>();
                }

                if (!neighbor_information.containsKey(end_id)) {
                    neighbor_information.put(end_id, costs[this.weight_index]);
                    gp_metis_formation.put(start_id, neighbor_information);
                    this.number_of_edges++;
                }


                //deal with the edge end_id --> start_id
                HashMap<Long, Double> reverse_neighbor_information;
                if (gp_metis_formation.get(end_id) != null) {
                    reverse_neighbor_information = gp_metis_formation.get(end_id);
                } else {
                    reverse_neighbor_information = new HashMap<>();
                }

                if (!reverse_neighbor_information.containsKey(start_id)) {
                    reverse_neighbor_information.put(start_id, costs[this.weight_index]);
                    gp_metis_formation.put(end_id, reverse_neighbor_information);
                    this.number_of_edges++;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


        this.number_of_edges = edges.size();
    }

    public void readTheNodesFromDisk(HashMap<Long, HashMap<Long, Double>> gp_metis_formation, String nodes_file) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(nodes_file));
            String line = null;

            while ((line = br.readLine()) != null) {
                long node_id = Long.parseLong(line.split(" ")[0]) + 1; // metis node id starts from 1
                gp_metis_formation.put(node_id, null);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean existedEdges(Pair<Long, Long> relations, HashMap<Pair<Long, Long>, double[]> edges) {
        long sid = relations.getKey();
        long did = relations.getValue();
        Pair<Long, Long> reverse_rel = new Pair<>(did, sid);
        return edges.containsKey(reverse_rel) || edges.containsKey(relations);
    }


//    class Edge {
//        double costs[];
//        long start_id;
//        long end_id;
//
//        public Edge(long start_id, long end_id, double[] costs) {
//            this.start_id = start_id;
//            this.end_id = end_id;
//            this.costs = new double[costs.length];
//            System.arraycopy(costs, 0, this.costs, 0, costs.length);
//        }
//    }

}
