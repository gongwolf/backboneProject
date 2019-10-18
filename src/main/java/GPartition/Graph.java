package GPartition;

import javafx.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.io.fs.FileUtils;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Graph {

    public int weight_index;
    public int graphsize;
    public int degree;
    public int dimension;
    public long number_of_nodes;
    public long number_of_edges;

    HashMap<PNode, HashMap<Long, double[]>> gp_metis_formation = new HashMap<>(); //  node id --> <neighbor id, costs>
    String graph_info_folder;


    int level;

    public Graph() {
        this.number_of_nodes = 0;
    }

    //Create the Graph from given folder
    public Graph(String graph_info_folder) {
        this.number_of_nodes = 0;
        this.level = 0;
        this.graph_info_folder = graph_info_folder;

        String[] path_infos = graph_info_folder.split("/");
        graphsize = Integer.parseInt(path_infos[path_infos.length - 2].split("_")[1]);
        degree = Integer.parseInt(path_infos[path_infos.length - 2].split("_")[2]);
        dimension = Integer.parseInt(path_infos[path_infos.length - 2].split("_")[3]);
        weight_index = Constants.weight_index;

        builtGraphFromFile(graph_info_folder);

    }

    private void builtGraphFromFile(String graph_info_folder) {
        String node_file = graph_info_folder + "/NodeInfo.txt";
        String edges_file = graph_info_folder + "/SegInfo.txt";
        readTheNodesFromDisk(this.gp_metis_formation, node_file);
        HashMap<Pair<Long, Long>, double[]> edges = new HashMap<>();
        readTheEdgesFromDisk(gp_metis_formation, edges_file, edges);
        this.number_of_nodes = gp_metis_formation.size();
        this.number_of_edges = edges.size();
    }

    private void readTheEdgesFromDisk(HashMap<PNode, HashMap<Long, double[]>> gp_metis_formation, String edges_file, HashMap<Pair<Long, Long>, double[]> edges) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(edges_file));
            String line = null;

            while ((line = br.readLine()) != null) {
//                System.out.println(line);
                String[] edge_info = line.split(" ");
                long start_id = Long.parseLong(edge_info[0]) + 1; // metis node id start from 1
                long end_id = Long.parseLong(edge_info[1]) + 1; // metis node is start from 1


                if (start_id == 1 || end_id == 1) {
                    System.out.println(line);
                }

                double[] costs = new double[this.dimension];
                costs[0] = Double.parseDouble(edge_info[2]);
                costs[1] = Double.parseDouble(edge_info[3]);
                costs[2] = Double.parseDouble(edge_info[4]);

                Pair<Long, Long> relations = new Pair<>(start_id, end_id);
                if (!existedEdges(relations, edges)) {
                    edges.put(relations, costs);
                }

                //deal with the edge start_id --> end_id
                HashMap<Long, double[]> neighbor_information;
                PNode node_key = new PNode(start_id, start_id - 1);
                PNode reverse_node_key = new PNode(end_id, end_id - 1);
                if (gp_metis_formation.get(node_key) != null) {
                    neighbor_information = gp_metis_formation.get(node_key);
                } else {
                    neighbor_information = new HashMap<>();
                }

                if (!neighbor_information.containsKey(end_id)) {
                    neighbor_information.put(end_id, costs);
                    gp_metis_formation.put(node_key, neighbor_information);
                    this.number_of_edges++;
                }

                //deal with the edge end_id --> start_id
                HashMap<Long, double[]> reverse_neighbor_information;
                if (gp_metis_formation.get(reverse_node_key) != null) {
                    reverse_neighbor_information = gp_metis_formation.get(reverse_node_key);
                } else {
                    reverse_neighbor_information = new HashMap<>();
                }

                if (!reverse_neighbor_information.containsKey(start_id)) {
                    reverse_neighbor_information.put(start_id, costs);
                    gp_metis_formation.put(reverse_node_key, reverse_neighbor_information);
                    this.number_of_edges++;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        this.number_of_edges = edges.size();
        System.out.println(gp_metis_formation.get(new PNode(1, 0)).size());
    }


    public void readTheNodesFromDisk(HashMap<PNode, HashMap<Long, double[]>> gp_metis_formation, String nodes_file) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(nodes_file));
            String line = null;

            while ((line = br.readLine()) != null) {
                long node_id = Long.parseLong(line.split(" ")[0]) + 1; // metis node id starts from 1
                gp_metis_formation.put(new PNode(node_id, node_id - 1), null); //current_id, neo4j_id
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {

        Graph ngraph = new Graph();

        ngraph.number_of_nodes = this.number_of_nodes;
        ngraph.number_of_edges = this.number_of_edges;

        ngraph.gp_metis_formation = new HashMap<>(this.gp_metis_formation);
        ngraph.level = this.level;
        ngraph.graph_info_folder = this.graph_info_folder;

        return (Object) ngraph;
    }

    /**
     * Split the graph into k(fan) partitions
     *
     * @param nparts the number of the partition that graph needs to be split.
     * @return the sub_graphs
     */
    public Graph[] split(int nparts) {
        System.out.println("Split the graph at level " + level + "  " + number_of_nodes + "  " + number_of_edges);
        Graph[] sub_graphs = new Graph[nparts];
        String target_path = graph_info_folder + "/metis_formation.graph";
        writeToDisk(gp_metis_formation, number_of_edges, target_path);
        callGPMetisCommand(nparts, target_path);

        String partitioned_file = graph_info_folder + "/metis_formation.graph.part." + nparts;
        HashMap<Long, Long> part_map = readPartitionResult(partitioned_file);

        TreeMap<PNode, HashMap<Long, double[]>> sorted = new TreeMap<>();
        sorted.putAll(gp_metis_formation);

        ArrayList<HashMap<PNode, HashMap<Long, double[]>>> sub_graphs_array_list = new ArrayList<>();
        for (int p = 0; p < nparts; p++) {
            HashMap<PNode, HashMap<Long, double[]>> gp_sub = new HashMap<>();
            sub_graphs_array_list.add(gp_sub);
        }


        for (Map.Entry<PNode, HashMap<Long, double[]>> e : sorted.entrySet()) {
            PNode key = e.getKey();
            long node_id = key.current_id;
            long part_no = part_map.get(node_id);

            HashMap<PNode, HashMap<Long, double[]>> gp_sub = sub_graphs_array_list.get((int) part_no);
            if (!gp_sub.containsKey(key)) {
                gp_sub.put(key, sorted.get(key));
            }
        }

        long sum = 0;
        for (int p = 0; p < nparts; p++) {
            int size = sub_graphs_array_list.get(p).size();
            sum+=size;
            System.out.println(size);
        }

        System.out.println(sum);

        return sub_graphs;
    }

    private void callGPMetisCommand(int nparts, String gp_graph_file) {
        String command = "gpmetis -objtype=vol " + gp_graph_file + " " + nparts;
        String ls_command = "ls " + graph_info_folder;
        ProcessBuilder pb = new ProcessBuilder("bash", "-c", command);
        try {
            pb.directory(new File(graph_info_folder));
            Process process = pb.start();

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

    private void writeToDisk(HashMap<PNode, HashMap<Long, double[]>> gp_metis_formation, long number_of_edges, String target_path) {
        this.number_of_nodes = gp_metis_formation.size();
        File target_file = new File(target_path);
        if (target_file.exists()) {
            target_file.delete();
        }

        try (FileWriter fw = new FileWriter(target_path);
             BufferedWriter bw = new BufferedWriter(fw)) {
            bw.write(number_of_nodes + " " + number_of_edges + " " + Constants.fmt + " \n");

            TreeMap<PNode, HashMap<Long, double[]>> sorted = new TreeMap<>();
            sorted.putAll(gp_metis_formation);


            for (Map.Entry<PNode, HashMap<Long, double[]>> e : sorted.entrySet()) {
                StringBuffer sb = new StringBuffer();
                for (Map.Entry<Long, double[]> ne : e.getValue().entrySet()) {
                    sb.append(ne.getKey()).append(" ").append((int) ne.getValue()[weight_index]).append(" ");
                }
                String line = sb.toString().trim() + "\n";
                System.out.print(e.getKey().current_id + "   " + e.getKey().neo4j_id + "  " + line);

                bw.write(line);
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public HashMap<Long, Long> readPartitionResult(String partitioned_file) {
        HashMap<Long, Long> part_map = new HashMap<>();
        BufferedReader br;
        long node_id = 1;
        try {
            br = new BufferedReader(new FileReader(partitioned_file));
            String line;
            while ((line = br.readLine()) != null) {
                Long parition_id = Long.parseLong(line);
                part_map.put(node_id, parition_id);
                node_id++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return part_map;
    }


    private boolean existedEdges(Pair<Long, Long> relations, HashMap<Pair<Long, Long>, double[]> edges) {
        long sid = relations.getKey();
        long did = relations.getValue();
        Pair<Long, Long> reverse_rel = new Pair<>(did, sid);
        return edges.containsKey(reverse_rel) || edges.containsKey(relations);
    }
}


class PNode implements Comparable {
    long current_id; // the current id that is used by the gpmetis code
    long neo4j_id; // the node in neo4j

    public PNode(long current_id, long neo4j_id) {
        this.current_id = current_id;
        this.neo4j_id = neo4j_id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PNode pNode = (PNode) o;
        return current_id == pNode.current_id &&
                neo4j_id == pNode.neo4j_id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(current_id, neo4j_id);
    }

    @Override
    public int compareTo(Object o) {
        PNode o_node = (PNode) o;
        return (int) (current_id - o_node.current_id);
    }
}