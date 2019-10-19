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

    HashMap<PNode, HashMap<PNode, double[]>> gp_metis_formation = new HashMap<>(); //  node id --> <neighbor id, costs>
    String graph_info_folder;


    int level;
    public long borderNumber;

    public Graph() {
        this.number_of_nodes = 0;
        this.number_of_edges = 0;
    }

    public Graph(HashMap<PNode, HashMap<PNode, double[]>> gp_metis_formation, long edge_numbers) {
        this.number_of_nodes = gp_metis_formation.size();
        this.number_of_edges = edge_numbers;
        this.gp_metis_formation.putAll(gp_metis_formation);

    }

    /**
     * Create the Graph from given folder
     * The graph at level 0, is the original graph.
     * current id is the id that is used to metis partition.
     * neo4j id is the actual that store by neo4j
     **/
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
        readTheNodesFromDisk(gp_metis_formation, node_file);
        HashMap<Pair<Long, Long>, double[]> edges = new HashMap<>();
        readTheEdgesFromDisk(gp_metis_formation, edges_file, edges);
        this.number_of_nodes = gp_metis_formation.size();
        this.number_of_edges = edges.size();
    }

    private void readTheEdgesFromDisk(HashMap<PNode, HashMap<PNode, double[]>> gp_metis_formation, String edges_file, HashMap<Pair<Long, Long>, double[]> edges) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(edges_file));
            String line = null;

            while ((line = br.readLine()) != null) {
//                System.out.println(line);
                String[] edge_info = line.split(" ");
                long start_id = Long.parseLong(edge_info[0]) + 1; // metis node id start from 1
                long end_id = Long.parseLong(edge_info[1]) + 1; // metis node is start from 1

//
//                if (start_id == 1 || end_id == 1) {
//                    System.out.println(line);
//                }

                double[] costs = new double[this.dimension];
                costs[0] = Double.parseDouble(edge_info[2]);
                costs[1] = Double.parseDouble(edge_info[3]);
                costs[2] = Double.parseDouble(edge_info[4]);

                Pair<Long, Long> relations = new Pair<>(start_id, end_id);
                if (!existedEdges(relations, edges)) {
                    edges.put(relations, costs);
                }


                PNode node_key = new PNode(start_id, start_id - 1);
                PNode reverse_node_key = new PNode(end_id, end_id - 1);

                //deal with the edge start_id --> end_id
                HashMap<PNode, double[]> neighbor_information;
                if (gp_metis_formation.get(node_key) != null) {
                    neighbor_information = gp_metis_formation.get(node_key);
                } else {
                    neighbor_information = new HashMap<>();
                }

                PNode end_node_key = new PNode(end_id, end_id - 1);
                if (!neighbor_information.containsKey(end_node_key)) {
                    neighbor_information.put(end_node_key, costs);
                    gp_metis_formation.put(node_key, neighbor_information);
                    this.number_of_edges++;
                }

                //deal with the edge end_id --> start_id
                HashMap<PNode, double[]> reverse_neighbor_information;
                if (gp_metis_formation.get(reverse_node_key) != null) {
                    reverse_neighbor_information = gp_metis_formation.get(reverse_node_key);
                } else {
                    reverse_neighbor_information = new HashMap<>();
                }

                PNode start_node_key = new PNode(start_id, start_id - 1);
                if (!reverse_neighbor_information.containsKey(start_node_key)) {
                    reverse_neighbor_information.put(start_node_key, costs);
                    gp_metis_formation.put(reverse_node_key, reverse_neighbor_information);
                    this.number_of_edges++;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        this.number_of_edges = edges.size();
    }


    public void readTheNodesFromDisk(HashMap<PNode, HashMap<PNode, double[]>> gp_metis_formation, String nodes_file) {
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
        ngraph.borderNumber = this.borderNumber;

        ngraph.gp_metis_formation = new HashMap<>(this.gp_metis_formation);
        ngraph.level = this.level;
        ngraph.graph_info_folder = this.graph_info_folder;
        return (Object) ngraph;
    }

    public void setGraph_info_folder(String graph_info_folder) {
        this.graph_info_folder = graph_info_folder;
        String[] path_infos = graph_info_folder.split("/");
        graphsize = Integer.parseInt(path_infos[path_infos.length - 2].split("_")[1]);
        degree = Integer.parseInt(path_infos[path_infos.length - 2].split("_")[2]);
        dimension = Integer.parseInt(path_infos[path_infos.length - 2].split("_")[3]);
        weight_index = Constants.weight_index;
    }

    private void setBorderNumber(long borderNumber) {
        this.borderNumber = borderNumber;
    }

    /**
     * Split the graph into k(fan) partitions
     *
     * @param nparts the number of the partition that graph needs to be split.
     * @return the sub_graphs
     */
    public Graph[] split(int nparts) {
//        System.out.println("Split the graph at level " + level + "  " + number_of_nodes + "  " + number_of_edges+"  "+borderNumber);
        Graph[] sub_graphs = new Graph[nparts];
        String target_path = graph_info_folder + "/metis_formation.graph";
        writeToDisk(gp_metis_formation, number_of_edges, target_path);
        callGPMetisCommand(nparts, target_path);

        String partitioned_file = graph_info_folder + "/metis_formation.graph.part." + nparts;
        HashMap<Long, Long> part_map = readPartitionResult(partitioned_file);

        TreeMap<PNode, HashMap<PNode, double[]>> sorted = new TreeMap<>();
        sorted.putAll(gp_metis_formation);

        HashMap<Long, HashMap<PNode, HashMap<PNode, double[]>>> sub_graphs_array_list = new HashMap<>();
        ArrayList<HashMap<Long, Long>> node_id_part_mapping = new ArrayList<>();


        for (int p = 0; p < nparts; p++) {
            HashMap<PNode, HashMap<PNode, double[]>> gp_sub = new HashMap<>();
            sub_graphs_array_list.put((long) p, gp_sub);

            HashMap<Long, Long> node_mapping = new HashMap<>();
            node_id_part_mapping.add(node_mapping);
        }


        for (Map.Entry<PNode, HashMap<PNode, double[]>> e : sorted.entrySet()) {
            PNode key = e.getKey();
            long node_id = key.current_id;
            long part_no = part_map.get(node_id);
            HashMap<Long, Long> node_map_in_part = node_id_part_mapping.get((int) part_no);
            long new_node_id = node_map_in_part.size() + 1;
            node_map_in_part.put(node_id, new_node_id);
        }


        long[] edge_numbers = new long[nparts];
        long cross_edge_numbers = 0;
        long[] border_nodes_number = new long[nparts];
        long[] non_border_nodes_number = new long[nparts];

        for (Map.Entry<PNode, HashMap<PNode, double[]>> e : sorted.entrySet()) {
            long node_id = e.getKey().current_id;
            long part_no = part_map.get(node_id);

            HashMap<PNode, HashMap<PNode, double[]>> gp_sub = sub_graphs_array_list.get(part_no);
            HashMap<Long, Long> node_mapping = node_id_part_mapping.get((int) part_no);

            long new_node_id = node_mapping.get(node_id);

            PNode new_key = new PNode(new_node_id, e.getKey().neo4j_id);

            if (!gp_sub.containsKey(new_key)) {
                HashMap<PNode, double[]> neighborsInSamePartition = new HashMap<>();
                HashMap<PNode, double[]> old_neighbors = gp_metis_formation.get(e.getKey());
                for (Map.Entry<PNode, double[]> edge : old_neighbors.entrySet()) {
                    long old_id = edge.getKey().current_id;
                    long old_neo4j_id = edge.getKey().neo4j_id;
                    long edge_end_part_no = part_map.get(old_id);

                    //Two nodes pf the edgs belong to same partition
                    if (edge_end_part_no == part_no) {
                        long new_end_id = node_mapping.get(old_id);
                        PNode end_pnode = new PNode(new_end_id, old_neo4j_id);
                        neighborsInSamePartition.put(end_pnode, edge.getValue());
                        edge_numbers[(int) part_no]++;
                    } else {
                        cross_edge_numbers++;
                    }
                }

                if (e.getValue().size() != neighborsInSamePartition.size()) {
                    border_nodes_number[(int) part_no]++;
                } else {
                    non_border_nodes_number[(int) part_no]++;
                }

                gp_sub.put(new_key, neighborsInSamePartition);
            }

            sub_graphs_array_list.put(part_no, gp_sub);

        }

        long sum_border = 0;
        for (int p = 0; p < nparts; p++) {
            Graph s_g = new Graph(sub_graphs_array_list.get((long) p), edge_numbers[p] / 2);
//            System.out.println("::::::" + sub_graphs_array_list.get((long) p).size());
            s_g.setGraph_info_folder(graph_info_folder);
//            s_g.setBorderNumber(border_nodes_number[p]);
            sum_border += border_nodes_number[p];
            s_g.level = this.level + 1;
            sub_graphs[p] = s_g;
            if (s_g.number_of_nodes <= Constants.vertex_in_leaf) {
                s_g.setBorderNumber(border_nodes_number[p]);
            }
        }

//        System.out.println(sum_border);
        this.setBorderNumber(sum_border);
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
//            if (exitVal == 0) {
//                System.out.println("Success!");
//            } else {
//                System.out.println("failure !!!!");
//            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void writeToDisk(HashMap<PNode, HashMap<PNode, double[]>> gp_metis_formation, long number_of_edges, String target_path) {
        this.number_of_nodes = gp_metis_formation.size();
        File target_file = new File(target_path);
        if (target_file.exists()) {
            target_file.delete();
        }

        try (FileWriter fw = new FileWriter(target_path);
             BufferedWriter bw = new BufferedWriter(fw)) {
            bw.write(number_of_nodes + " " + number_of_edges + " " + Constants.fmt + " \n");

            TreeMap<PNode, HashMap<PNode, double[]>> sorted = new TreeMap<>();
            sorted.putAll(gp_metis_formation);


            for (Map.Entry<PNode, HashMap<PNode, double[]>> e : sorted.entrySet()) {
                StringBuffer sb = new StringBuffer();
                for (Map.Entry<PNode, double[]> ne : e.getValue().entrySet()) {
                    int value = (int) ne.getValue()[weight_index];

                    value = value >= 1 ? value : 1;

                    sb.append(ne.getKey().current_id).append(" ").append(value).append(" ");
                }
                String line = sb.toString().trim() + "\n";

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