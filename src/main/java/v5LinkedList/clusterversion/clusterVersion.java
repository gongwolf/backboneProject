package v5LinkedList.clusterversion;

import Neo4jTools.Line;
import Neo4jTools.Neo4jDB;
import Query.backbonePath;
import configurations.ProgramProperty;
import javafx.util.Pair;
import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.*;
import v5LinkedList.*;

import java.io.*;
import java.util.*;

public class clusterVersion {

    private Neo4jDB neo4j;
    private GraphDatabaseService graphdb;
    private long cn; //number of graph nodes
    private long numberOfEdges; // number of edges ;
    //    private DynamicForests dforests;
    private double percentage;
    public ProgramProperty prop = new ProgramProperty();
    public String city_name;
    //    public String base_db_name = "sub_ny_USA";
    //    public String base_db_name = "col_USA";
//
//    public String base_db_name = "sub_ny_USA_50K";
//    public String folder_name = "busline_sub_graph_NY_50K";
    public String base_db_name = "sub_ny_USA";
    public String folder_name = "busline_sub_graph_NY";

    //Pair <sid_degree,did_degree> -> list of the relationship id that the degrees of the start node and end node are the response given pair of key
    TreeMap<Pair<Integer, Integer>, ArrayList<Long>> degree_pairs = new TreeMap(new PairComparator());

    //deleted edges record the relationship deleted in each layer, the index of each layer is based on the expansion on previous level graph
    ArrayList<HashSet<Long>> deletedEdges_layer = new ArrayList<>();
    private int min_size = 100; // the size of the cluster, the limitation of each level needs to be deleted
    private int degree_pairs_sum;


    public static void main(String args[]) throws CloneNotSupportedException {
        long start = System.currentTimeMillis();
        clusterVersion index = new clusterVersion();
        index.percentage = 0.01;
        index.city_name = "ny_USA";
        index.build();
        long end = System.currentTimeMillis();
        System.out.println("Total Running time " + (end - start) * 1.0 / 1000 + "s  ~~~~~ ");
    }


    private void build() {
        initLevel();
        createIndexFolder();
        construction();
//        createIndexFolder();
//        indexBuild();
    }

    private void construction() {

        int currentLevel = 0;

        boolean nodes_deleted = false;

        do {
            int upperlevel = currentLevel + 1;
            System.out.println("===============  level:" + upperlevel + " ==============");

            //copy db from previous level
            copyToHigherDB(currentLevel, upperlevel);

            //handle the degrees pairs
            nodes_deleted = handleUpperLevelGraph(upperlevel);

            currentLevel = upperlevel;
        } while (nodes_deleted);
//        } while (currentLevel != 2);

        System.out.println("finish the index finding, the current level is " + this.deletedEdges_layer.size() + "  with number of nodes :" + cn);
    }


    private boolean handleUpperLevelGraph(int currentLevel) {
        String sub_db_name = this.base_db_name + "_Level" + currentLevel;

        String prefix = "/home/gqxwolf/mydata/projectData/BackBone/";

        neo4j = new Neo4jDB(sub_db_name);
        neo4j.startDB(false);
        graphdb = neo4j.graphDB;
        long pre_n = neo4j.getNumberofNodes();
        long pre_e = neo4j.getNumberofEdges();
        System.out.println("deal with level " + currentLevel + " graph at " + neo4j.DB_PATH + "  " + pre_n + " nodes and " + pre_e + " edges");


        // record the nodes that are deleted in this layer.
        HashSet<Long> deletedNodes = new HashSet<>();
        // record the edges that are deleted in this layer, the index is build based on it
        HashSet<Long> deletedEdges = new HashSet<>();
        boolean deleted = false;

        HashSet<Long> sub_step_deletedEdges = new HashSet<>(); // the deleted edges in this sub step
        HashSet<Long> sub_step_deletedNodes = new HashSet<>(); // the deleted nodes in this sub step

        do {

            sub_step_deletedEdges.clear();
            sub_step_deletedNodes.clear();

            if (currentLevel == 1) {
                getDegreePairs();
                sub_step_deletedEdges.addAll(removeSingletonEdges(neo4j, sub_step_deletedNodes));
                System.out.println("Finish finding the level 0 spanning tree..........");
                checkIndexFolderExisted(0);
                indexBuildAtLevel0(sub_step_deletedEdges, sub_step_deletedNodes);
            } else {
                System.out.println("Updated the neo4j database object ................");
            }

            System.out.println("#####");

            String textFilePath = prefix + folder_name + "/non-single/level" + currentLevel + "/";
            neo4j.saveGraphToTextFormation(textFilePath);

            //remove the edges in each cluster
            NodeClusters process_clusters = removeLowerDegreePairEdgesByThreshold(sub_step_deletedNodes, sub_step_deletedEdges);
            deleted = !sub_step_deletedEdges.isEmpty();

            System.out.println("Removing the edges in level " + currentLevel + "  with degree threshold  : ");
            long post_n = neo4j.getNumberofNodes();
            long post_e = neo4j.getNumberofEdges();
            System.out.println("~~~~~~~~~~~~~ pre:" + pre_n + " " + pre_e + "  post:" + post_n + " " + post_e + "   # of deleted Edges:" + sub_step_deletedEdges.size() + "   # of processed clusters " + process_clusters.getNumberOfClusters());


            getDegreePairs();
            sub_step_deletedEdges.addAll(removeSingletonEdgesInForests(sub_step_deletedNodes));
            long numberOfNodes = neo4j.getNumberofNodes();
            post_n = neo4j.getNumberofNodes();
            post_e = neo4j.getNumberofEdges();

            textFilePath = prefix + folder_name + "/level" + currentLevel + "/";
            System.out.println(textFilePath);
            neo4j.saveGraphToTextFormation(textFilePath);


            System.out.println(" ~~~~~~~~~~~~~ pre:" + pre_n + " " + pre_e + "  post:" + post_n + " " + post_e + "   # of deleted Edges:" + sub_step_deletedEdges.size() + " in the level   (" + min_size + ")  " + deleted);

            cn = numberOfNodes;

            if (cn == 0) {
                deleted = false;
            }


            deletedEdges.addAll(sub_step_deletedEdges);
            deletedNodes.addAll(sub_step_deletedNodes);

//
            int level = currentLevel - 1;
            checkIndexFolderExisted(level);


            System.out.println("Add the deleted edges of the level " + currentLevel + "  to the deletedEdges_layer structure. Build the index for level " + level);

            for (Map.Entry<Integer, NodeCluster> cluster_entry : process_clusters.clusters.entrySet()) {
                //jump the noise cluster
                if (cluster_entry.getKey() == 0) {
                    continue;
                }

                NodeCluster cluster = cluster_entry.getValue();
                long rt_index_build_st = System.currentTimeMillis();
                indexBuildAtLevel(level, sub_step_deletedEdges, cluster);
                long rt_index_build = System.currentTimeMillis() - rt_index_build_st;
                System.out.println("index building " + rt_index_build + "(ms) --- on cluster:" + cluster.cluster_id + "   cluster size: " + cluster.node_list.size() + "   # of rels in cluster:" + cluster.rels.size() + "   # of border nodes:" + cluster.border_node_list.size());
            }

        } while (deleted && deletedEdges.size() <= this.percentage * this.numberOfEdges && sub_step_deletedEdges.size() != 0);

        neo4j.closeDB();

        if (!deletedEdges.isEmpty() && sub_step_deletedEdges.size() != 0) {
            this.deletedEdges_layer.add(deletedEdges);
            System.out.println("Deleted # of edges in this level " + deletedEdges.size() + "  the # of edges are removed in the last step in this level is " + sub_step_deletedEdges.size());
            return true;
        } else {
            System.out.println("Can not delete the edges in this level, stop the abstraction of the graph.   " + deletedEdges.size() + "  " + sub_step_deletedEdges.size());
//            if (!deletedEdges.isEmpty()) {
//                System.out.println("-----------------------------------------------------------------------------------------");
//                System.out.println("The edges are deleted but can not build any higher level, build the last level graph ");
//                int final_level = currentLevel + 1;
//                copyToHigherDB(currentLevel, final_level);
//                String graph_db_folder = this.base_db_name + "_Level" + final_level;
//                Neo4jDB neo4j_level = new Neo4jDB(graph_db_folder);
//                neo4j_level.startDB(true);
//                GraphDatabaseService graphdb_level = neo4j_level.graphDB;
//
//                String textFilePath = prefix + folder_name + "/level" + final_level + "/";
//                System.out.println(textFilePath);
//                neo4j_level.saveGraphToTextFormation(textFilePath);
//                System.out.println("The final level graph is constructed: level " + final_level + "   # of nodes:" + neo4j_level.getNumberofNodes() + "  # of edges:" + neo4j_level.getNumberofEdges());
//                neo4j_level.closeDB();
//            }
            return false;
        }
    }

    private void checkIndexFolderExisted(int indexLevel) {

        String sub_folder_str = "/home/gqxwolf/mydata/projectData/BackBone/indexes/" + this.folder_name + "/level" + indexLevel;

        File sub_folder_f = new File(sub_folder_str);
        if (!sub_folder_f.exists()) {
            sub_folder_f.mkdirs();
        }
    }

    /**
     * Find the index at @level, which means the information that is abstracted from previous level to current level
     *
     * @param level   the level of the current index
     * @param de      the edges are deleted in current sub step
     * @param cluster which cluster the index needs to be built in
     */
    private void indexBuildAtLevel(int level, HashSet<Long> de, NodeCluster cluster) {
        int previous_level = level;

        //find all the information in previous level graph, because the information is removed in current layer graph
        String graph_db_folder = this.base_db_name + "_Level" + previous_level;
        Neo4jDB neo4j_level = new Neo4jDB(graph_db_folder);
        neo4j_level.startDB(true);
        GraphDatabaseService graphdb_level = neo4j_level.graphDB;

        //check the node remained in after the deletion (remained in current layer graph)
        HashSet<Long> remained_nodes = getNodeListAtLevel(cluster.node_list);

        try (Transaction tx = graphdb_level.beginTx()) {

            for (long n_id : cluster.node_list) {

                HashMap<Long, ArrayList<double[]>> skylines = readIndex(n_id, level);

                HashMap<Long, myQueueNode> tmpStoreNodes = new HashMap();
                myQueueNode snode = new myQueueNode(n_id, neo4j_level);
                myNodePriorityQueue mqueue = new myNodePriorityQueue();
                tmpStoreNodes.put(snode.id, snode);
                mqueue.add(snode);

//                System.out.println(snode + "   " + (skylines == null));

                while (!mqueue.isEmpty()) {
                    myQueueNode v = mqueue.pop();
                    for (int i = 0; i < v.skyPaths.size(); i++) {
                        path p = v.skyPaths.get(i);
                        if (!p.expaned) {
                            p.expaned = true;

                            ArrayList<path> new_paths;
                            new_paths = p.expand(neo4j_level, de, cluster.rels);

                            for (path np : new_paths) {
//                                System.out.println("            "+np);
                                myQueueNode next_n;
                                if (tmpStoreNodes.containsKey(np.endNode)) {
                                    next_n = tmpStoreNodes.get(np.endNode);
                                } else {
                                    next_n = new myQueueNode(snode, np.endNode, neo4j_level);
                                    tmpStoreNodes.put(next_n.id, next_n);
                                }

                                if (next_n.addToSkyline(np) && !next_n.inqueue) {
                                    mqueue.add(next_n);
                                    next_n.inqueue = true;
                                }
                            }
                        }
                    }
                }

                int sum = 0;
                for (Map.Entry<Long, myQueueNode> e : tmpStoreNodes.entrySet()) {
                    ArrayList<path> sk = e.getValue().skyPaths;
                    //remove the index of the self connection that node only has one skyline path and the skyline path is to itself
                    if (!(sk.size() == 1 && sk.get(0).costs[0] == 0 && sk.get(0).costs[1] == 0 && sk.get(0).costs[2] == 0)) {
                        for (path p : sk) {
                            if (remained_nodes.contains(p.endNode)) {
                                sum++;
                            }
                        }
                    }
                }

                if (sum != 0) {
                    /**clean the built index file*/
                    String sub_folder_str = "/home/gqxwolf/mydata/projectData/BackBone/indexes/" + this.folder_name + "/level" + level;
                    File idx_file = new File(sub_folder_str + "/" + n_id + ".idx");

                    //previous has the skyline information from n_id to other nodes (deleted in previous sub step)
                    if (skylines != null) {

                        for (Map.Entry<Long, myQueueNode> e : tmpStoreNodes.entrySet()) {
                            long target_node_id = e.getKey();
                            myQueueNode node_obj = e.getValue();
                            ArrayList<path> skys = node_obj.skyPaths;
                            for (path p : skys) {
                                /** the end node of path is a highway, the node is still appear in next level, also, the path is not a dummy path of source node **/
                                if (p.endNode != n_id) {
                                    if (remained_nodes.size() != 0 && remained_nodes.contains(p.endNode)) { // not the max_level graph, have reminding nodes on next level
                                        double[] costs = new double[3];
                                        costs[0] = p.costs[0];
                                        costs[1] = p.costs[1];
                                        costs[2] = p.costs[2];

                                        if (skylines.containsKey(target_node_id)) {
                                            ArrayList<double[]> skyline_costs = skylines.get(target_node_id);
                                            addToSkyline(skyline_costs, costs);
                                            skylines.put(target_node_id, skyline_costs);
                                        } else {
                                            ArrayList<double[]> skyline_costs = new ArrayList<>();
                                            skyline_costs.add(costs);
                                            skylines.put(target_node_id, skyline_costs);
                                        }

                                    }
                                }
                            }
                        }

                        BufferedWriter writer = new BufferedWriter(new FileWriter(idx_file.getAbsolutePath()));
                        for (Map.Entry<Long, ArrayList<double[]>> e : skylines.entrySet()) {
                            long target_node_id = e.getKey();
                            ArrayList<double[]> skys = e.getValue();
                            for (double[] costs : skys) {
                                if (target_node_id != n_id) {
                                    if (remained_nodes.size() != 0 && remained_nodes.contains(target_node_id)) {
                                        writer.write(target_node_id + " " + costs[0] + " " + costs[1] + " " + costs[2] + "\n");
                                        System.out.println(n_id + " " + target_node_id + " " + costs[0] + " " + costs[1] + " " + costs[2]);
                                    }
                                }
                            }
                        }
                        writer.close();


                    } else {
                        BufferedWriter writer = new BufferedWriter(new FileWriter(idx_file.getAbsolutePath()));
                        for (Map.Entry<Long, myQueueNode> e : tmpStoreNodes.entrySet()) {
                            long target_node_id = e.getKey();
                            myQueueNode node_obj = e.getValue();
                            ArrayList<path> skys = node_obj.skyPaths;
                            for (path p : skys) {
                                /** the end node of path is a highway, the node is still appear in next level, also, the path is not a dummy path of source node **/
                                if (p.endNode != n_id) {
                                    if (remained_nodes.size() != 0 && remained_nodes.contains(p.endNode)) { // not the max_level graph, have reminding nodes on next level
                                        writer.write(target_node_id + " " + p.costs[0] + " " + p.costs[1] + " " + p.costs[2] + "\n");
                                    }
                                }
                            }
                        }
                        writer.close();

                    }
                }
            }
            tx.success();
        } catch (IOException e) {
            e.printStackTrace();
        }
        neo4j_level.closeDB();
    }


    private void indexBuildAtLevel0(HashSet<Long> sub_step_deletedEdges, HashSet<Long> sub_step_deletedNodes) {
        int level = 0;
        //find all the information in previous level graph, because the information is removed in current layer graph
        String graph_db_folder = this.base_db_name + "_Level0";
        Neo4jDB neo4j_level = new Neo4jDB(graph_db_folder);
        neo4j_level.startDB(true);
        GraphDatabaseService graphdb_level = neo4j_level.graphDB;

        //check the node remained in after the deletion (remained in current layer graph)
        HashSet<Long> remained_nodes = getNodeListAtLevel();

        try (Transaction tx = graphdb_level.beginTx()) {
            for (long n_id : sub_step_deletedNodes) {
                HashMap<Long, myQueueNode> tmpStoreNodes = new HashMap();
                myQueueNode snode = new myQueueNode(n_id, neo4j_level);
                myNodePriorityQueue mqueue = new myNodePriorityQueue();
                tmpStoreNodes.put(snode.id, snode);
                mqueue.add(snode);

                while (!mqueue.isEmpty()) {
                    myQueueNode v = mqueue.pop();
//                    System.out.println(v.id);
                    for (int i = 0; i < v.skyPaths.size(); i++) {
                        path p = v.skyPaths.get(i);
//                        System.out.println(p);
                        if (!p.expaned) {
                            p.expaned = true;
                            ArrayList<path> new_paths;
                            new_paths = p.expand(neo4j_level, sub_step_deletedEdges, sub_step_deletedEdges);
                            for (path np : new_paths) {
                                myQueueNode next_n;
//                                System.out.println("    " + np);
                                if (tmpStoreNodes.containsKey(np.endNode)) {
                                    next_n = tmpStoreNodes.get(np.endNode);
                                } else {
                                    next_n = new myQueueNode(snode, np.endNode, neo4j_level);
                                    tmpStoreNodes.put(next_n.id, next_n);
                                }

                                if (next_n.addToSkyline(np) && !next_n.inqueue) {
                                    mqueue.add(next_n);
                                    next_n.inqueue = true;
                                }
                            }
                        }
                    }
                }

                int sum = 0;
                for (Map.Entry<Long, myQueueNode> e : tmpStoreNodes.entrySet()) {
                    ArrayList<path> sk = e.getValue().skyPaths;
                    //remove the index of the self connection that node only has one skyline path and the skyline path is to itself
                    if (!(sk.size() == 1 && sk.get(0).costs[0] == 0 && sk.get(0).costs[1] == 0 && sk.get(0).costs[2] == 0)) {
                        for (path p : sk) {
                            if (remained_nodes.contains(p.endNode)) {
//                                System.out.println(e.getKey() + "    " + p);
                                sum++;
                            }
                        }
                    }
                }

                if (sum != 0) {
                    /**clean the built index file*/
                    String sub_folder_str = "/home/gqxwolf/mydata/projectData/BackBone/indexes/" + this.folder_name + "/level" + level;
                    File idx_file = new File(sub_folder_str + "/" + n_id + ".idx");

                    //previous has the skyline information from n_id to other nodes (deleted in previous sub step)

                    BufferedWriter writer = new BufferedWriter(new FileWriter(idx_file.getAbsolutePath()));
                    for (Map.Entry<Long, myQueueNode> e : tmpStoreNodes.entrySet()) {
                        long target_node_id = e.getKey();
                        myQueueNode node_obj = e.getValue();
                        ArrayList<path> skys = node_obj.skyPaths;
                        for (path p : skys) {
                            /** the end node of path is a highway, the node is still appear in next level, also, the path is not a dummy path of source node **/
                            if (p.endNode != n_id) {
                                if (remained_nodes.size() != 0 && remained_nodes.contains(p.endNode)) { // not the max_level graph, have reminding nodes on next level
                                    writer.write(target_node_id + " " + p.costs[0] + " " + p.costs[1] + " " + p.costs[2] + "\n");
                                }
                            }
                        }
                    }
                    writer.close();
                }

            }
            tx.success();
        } catch (IOException e) {
            e.printStackTrace();
        }
        neo4j_level.closeDB();
    }


    private HashMap<Long, ArrayList<double[]>> readIndex(long n_id, int level) {
        HashMap<Long, ArrayList<double[]>> skylines = new HashMap<>();
        String sub_folder_str = "/home/gqxwolf/mydata/projectData/BackBone/indexes/" + this.folder_name + "/level" + level;
        File idx_file = new File(sub_folder_str + "/" + n_id + ".idx");
        try {
            if (idx_file.exists()) {
                BufferedReader b = new BufferedReader(new FileReader(idx_file));
                String readLine = "";
                while ((readLine = b.readLine()) != null) {
                    String[] infos = readLine.split(" ");
                    double[] costs = new double[infos.length - 1];
                    long target_node = Long.parseLong(infos[0]);
                    costs[0] = Double.parseDouble(infos[1]);
                    costs[1] = Double.parseDouble(infos[2]);
                    costs[2] = Double.parseDouble(infos[3]);

                    if (skylines.containsKey(target_node)) {
                        ArrayList<double[]> skyline_costs = skylines.get(target_node);
                        skyline_costs.add(costs);
                        skylines.put(target_node, skyline_costs);
                    } else {
                        ArrayList<double[]> skyline_costs = new ArrayList<>();
                        skyline_costs.add(costs);
                        skylines.put(target_node, skyline_costs);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (skylines.size() != 0) {
            return skylines;
        } else {
            return null;
        }
    }

    private NodeClusters removeLowerDegreePairEdgesByThreshold(HashSet<Long> deletedNodes, HashSet<Long> deletedEdges) {
        NodeClusters result_clusters = new NodeClusters();

        HashMap<Long, myNode> visited_nodes = new HashMap<>();

        NodeClusters node_clusters = new NodeClusters();

        HashMap<Long, NodeCoefficient> node_coefficient_list = getNodesCoefficientList();

//        HashMap<Long, NodeCoefficient> sorted_coefficient = CollectionOperations.sortHashMapByValue(node_coefficient_list);

//        sorted_coefficient.forEach((k, v) -> System.out.println(k + "  " + v.coefficient));

        TreeMap<Integer, Integer> node_neighbor_number_distribution = new TreeMap<>(); //the distribution of the two-neighbor coefficient


        for (Map.Entry<Long, NodeCoefficient> node_coeff : node_coefficient_list.entrySet()) {
            if (node_neighbor_number_distribution.containsKey(node_coeff.getValue().getNumberOfTwoHopNeighbors())) {
                node_neighbor_number_distribution.put(node_coeff.getValue().getNumberOfTwoHopNeighbors(), node_neighbor_number_distribution.get(node_coeff.getValue().getNumberOfTwoHopNeighbors()) + 1);
            } else {
                node_neighbor_number_distribution.put(node_coeff.getValue().getNumberOfTwoHopNeighbors(), 1);
            }
        }

        int dis_indicator = 0;
        int noise_indicator = 0;
        for (Map.Entry<Integer, Integer> e : node_neighbor_number_distribution.entrySet()) {
            dis_indicator += e.getValue();
            if ((1.0 * dis_indicator / node_coefficient_list.size()) * 100 > 30) {
                break;
            } else {
                noise_indicator = e.getKey();
            }
        }

        System.out.println(node_coefficient_list.size() + "            indicator ::: " + noise_indicator);


        for (Map.Entry<Long, NodeCoefficient> node_coeff : node_coefficient_list.entrySet()) {

            try (Transaction tx = this.neo4j.graphDB.beginTx()) {
                long node_id = node_coeff.getKey();


                if (node_clusters.isInClusters(node_id)) {
                    continue;
                }

                if (node_coeff.getValue().getNumberOfTwoHopNeighbors() <= noise_indicator) {
                    if (!visited_nodes.containsKey(node_id)) {
                        myNode next_node = new myNode(node_id, this.neo4j.graphDB.getNodeById(node_id), node_coefficient_list.get(node_id).coefficient);
                        visited_nodes.put(node_id, next_node);
                    }
                    node_clusters.clusters.get(0).addToCluster(node_id);
                    continue;
                }


                double coefficient = node_coefficient_list.get(node_id).coefficient;

                NodeCluster cluster = new NodeCluster(node_clusters.getNextClusterID());

                myClusterQueue queue = new myClusterQueue();

                myNode m_node = new myNode(node_id, graphdb.getNodeById(node_id), coefficient);

                queue.add(m_node);
                visited_nodes.put(node_id, m_node);

                while (!queue.isEmpty()) {
                    myNode n = queue.pop();

                    if (node_clusters.clusters.get(0).node_list.contains(n.id)) { // change the label of the node from noise to label c
                        node_clusters.clusters.get(0).node_list.remove(n.id);
                        cluster.addToCluster(n.id);
                    } else if (node_clusters.isInClusters(n.id) || cluster.isInCluster(n.id)) { // if n already has label
                        continue;
                    } else {
                        cluster.addToCluster(n.id);
                    }

                    ArrayList<Node> neighbors = neo4j.getNeighborsNodeList(n.id);

                    boolean can_add_to_queue = !cluster.oversize(this.min_size);

                    for (Node neighbor_node : neighbors) {
                        if (can_add_to_queue) {
                            NodeCoefficient n_coff = node_coefficient_list.get(neighbor_node.getId());

                            myNode next_node;
                            if (visited_nodes.containsKey(neighbor_node.getId())) {
                                next_node = visited_nodes.get(neighbor_node.getId());
                            } else {
                                next_node = new myNode(neighbor_node.getId(), neighbor_node, n_coff.coefficient);
                                visited_nodes.put(next_node.id, next_node);
                            }

                            if (n_coff.getNumberOfTwoHopNeighbors() > noise_indicator) {
                                queue.add(next_node);
                            }
                        }
                    }
                }

                node_clusters.clusters.put(cluster.cluster_id, cluster);
                tx.success();
            }
        }

        System.out.println("found # of clusters : " + node_clusters.clusters.size());

        System.out.println("The distribution of the two-hop node coefficient :   ");
        node_neighbor_number_distribution.forEach((k, v) -> System.out.println(k + "  " + v));

        node_clusters.clusters.forEach((k, v) -> v.updateBorderList(neo4j));

//        node_clusters.clusters.forEach((k, v) -> {
////            if (v.node_list.size() >= this.min_size && k != 0) {
//            System.out.println(k + "  " + v.node_list.size() + "  " + v.border_node_list.size());
////            }
//        });
//
//        node_clusters.clusters.forEach((k, v) -> {
//            if (v.node_list.size() >= 100 && k != 0) {
//                v.border_node_list.forEach(b_id -> System.out.println(b_id));
//            }
//        });
//
//
//        final int[] i = {0};
//        node_clusters.clusters.forEach((k, v) -> {
//            if (v.node_list.size() >= this.min_size && k != 0) {
//                v.node_list.forEach(node_id -> System.out.println(node_id + " " + i[0]));
//                i[0]++;
//            }
//        });

//        node_clusters.clusters.forEach((k, v) -> v.node_list.forEach(node_id -> System.out.println(node_id + " " + k)));
//
//
//        System.exit(0);
//
//        final int[] i = {0};
//        node_clusters.clusters.forEach((k, v) -> {
//            if (v.node_list.size() >= this.min_size && k != 0) {
////                v.node_list.forEach(node_id -> System.out.println(node_id + " " + i[0]));
//                NodeCluster cluster = new NodeCluster(i[0]);
//                cluster.addAll(v);
//                result_clusters.clusters.put(i[0], cluster);
//                i[0]++;
//            }
//        });

        System.out.println(neo4j.getNumberofNodes() + "   " + neo4j.getNumberofEdges());

        node_clusters.clusters.forEach((k, v) -> {
            if (v.node_list.size() >= this.min_size && k != 0) {
//                System.out.println("=================================================================================");
                System.out.print("Cluster :" + k + "  " + v.node_list.size() + " " + v.border_node_list.size());
                ClusterSpanningTree tree = new ClusterSpanningTree(neo4j, true, v.node_list);
                tree.EulerTourStringWiki();
                System.out.println("     size of spanning tree : " + tree.SpTree.size() + " ---- removed " + (tree.rels.size() - tree.SpTree.size()));

                NodeCluster cluster = new NodeCluster(k);
                cluster.addAll(v);
                cluster.addRels(tree.rels);
                result_clusters.clusters.put(k, cluster);

                try (Transaction tx = neo4j.graphDB.beginTx()) {

//                    for (long rel_id : tree.SpTree) {
//                        Relationship rel = neo4j.graphDB.getRelationshipById(rel_id);
//                        double start_node_lat = (double) rel.getStartNode().getProperty("lat");
//                        double start_node_lng = (double) rel.getStartNode().getProperty("log");
//                        double end_node_lat = (double) rel.getEndNode().getProperty("lat");
//                        double end_node_lng = (double) rel.getEndNode().getProperty("log");
//                        System.out.println(start_node_lat + " " + start_node_lng + " " + end_node_lat + " " + end_node_lng);
//                    }


                    for (long rel_id : tree.rels) {
                        Relationship rel = neo4j.graphDB.getRelationshipById(rel_id);
                        if (!tree.SpTree.contains(rel.getId())) {
                            deletedEdges.add(rel.getId());
                            deleteRelationshipFromDB(rel, deletedNodes);
                        }
                    }

                    tx.success();
                }
            }
        });

        System.out.println(neo4j.getNumberofNodes() + "   " + neo4j.getNumberofEdges());

        return result_clusters;
    }

    private void initLevel() {
//        String sub_db_name = "col_USA_Level0";
//        String sub_db_name = "sub_ny_USA_Level0";
        String sub_db_name = this.base_db_name + "_Level0";
        neo4j = new Neo4jDB(sub_db_name);
        System.out.println(neo4j.DB_PATH);
        neo4j.startDB(false);
        graphdb = neo4j.graphDB;
        getDegreePairs();

        cn = neo4j.getNumberofNodes();
        numberOfEdges = neo4j.getNumberofEdges();
        neo4j.closeDB();
//        this.min_size = (int) (this.percentage * this.numberOfEdges);
        //initialize the data structure that store multi-layer spanning forest.
//        this.dforests = new DynamicForests();
        System.out.println("Initialization: there are " + cn + " nodes and " + numberOfEdges + " edges" + "   " + this.min_size);
    }

    /**
     * Calculated the degree pair of each edge,
     * one distinct degree pair p contains:
     * the list of the edges whose degree pair of the start node and end node is equal to the given key p
     */
    private void getDegreePairs() {
        this.degree_pairs.clear();
        try (Transaction tx = graphdb.beginTx()) {
            ResourceIterable<Relationship> rels = this.graphdb.getAllRelationships();
            ResourceIterator<Relationship> rels_iter = rels.iterator();
            while (rels_iter.hasNext()) {
                Relationship r = rels_iter.next();
                int start_r = r.getStartNode().getDegree(Direction.BOTH);
                int end_r = r.getEndNode().getDegree(Direction.BOTH);

                if (start_r > end_r) {
                    int t = end_r;
                    end_r = start_r;
                    start_r = t;
                }

                Long rel_id = r.getId();
                Pair<Integer, Integer> p = new Pair<>(start_r, end_r);
                if (this.degree_pairs.containsKey(p)) {
                    ArrayList<Long> a = this.degree_pairs.get(p);
                    a.add(rel_id);
                    this.degree_pairs.put(p, a);
                } else {
                    ArrayList<Long> a = new ArrayList<>();
                    a.add(rel_id);
                    this.degree_pairs.put(p, a);
                }
            }

            this.degree_pairs_sum = Math.toIntExact(this.neo4j.getNumberofEdges());
            tx.success();
        }
    }


    /**
     * Copy the graph from the src_level to the dest_level,
     * the dest level graph used to shrink and build index on upper level.
     *
     * @param src_level
     * @param dest_level
     */
    private void copyToHigherDB(int src_level, int dest_level) {
        String src_db_name = this.base_db_name + "_Level" + src_level;
        String dest_db_name = this.base_db_name + "_Level" + dest_level;

        File src_db_folder = new File(prop.params.get("neo4jdb") + "/" + src_db_name);
        File dest_db_folder = new File(prop.params.get("neo4jdb") + "/" + dest_db_name);
        System.out.println("copy db from " + src_db_folder + " to " + dest_db_folder);
        try {
            if (dest_db_folder.exists()) {
                FileUtils.deleteDirectory(dest_db_folder);
            }
            FileUtils.copyDirectory(src_db_folder, dest_db_folder);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Remove the single edges from the first level graph where the db folder is Level0.
     *
     * @param neo4j
     * @param deletedNodes
     * @return
     */
    private HashSet<Long> removeSingletonEdges(Neo4jDB neo4j, HashSet<Long> deletedNodes) {
        HashSet<Long> deletedEdges = new HashSet<>();
        while (hasSingletonPairs()) {
            long pre_edge_num = neo4j.getNumberofEdges();
            long pre_node_num = neo4j.getNumberofNodes();
            long pre_degree_num = degree_pairs.size();
            int sum_single = 0;
            try (Transaction tx = graphdb.beginTx()) {
                //if the degree pair whose key or value is equal to 1, it means it is a single edge
                for (Map.Entry<Pair<Integer, Integer>, ArrayList<Long>> e : degree_pairs.entrySet()) {
                    if (e.getKey().getValue() == 1 || e.getKey().getKey() == 1) {
                        sum_single += e.getValue().size();
                        for (Long rel_id : e.getValue()) {
                            Relationship r = graphdb.getRelationshipById(rel_id);
                            /**
                             * although single edges are deleted, the remove type is multiple.
                             * It is used to make sure the highway knows the information of connection between two highway nodes.
                             * See the OneNote "backbone  --> Index organization"
                             * **/
                            deleteRelationshipFromDB(r, deletedNodes);
                            deletedEdges.add(r.getId());
                        }
                    }
                }
                tx.success();
            }

            getDegreePairs();
            System.out.println("delete single : pre:" + pre_node_num + " " + pre_edge_num + " " + pre_degree_num + " " +
                    "single_edges:" + sum_single + " " +
                    "post:" + neo4j.getNumberofNodes() + " " + neo4j.getNumberofEdges() + " " +
                    "dgr_paris:" + degree_pairs.size());
        }
        return deletedEdges;
    }


    private HashSet<Long> removeSingletonEdgesInForests(HashSet<Long> deletedNodes) {
        HashSet<Long> deletedEdges = new HashSet<>();
        while (hasSingletonPairs()) {
            long pre_edge_num = neo4j.getNumberofEdges();
            long pre_node_num = neo4j.getNumberofNodes();
            long pre_degree_num = degree_pairs.size();
            int sum_single = 0;
            try (Transaction tx = graphdb.beginTx()) {
                //if the degree pair whose key or value is equal to 1, it means it is a single edge
                for (Map.Entry<Pair<Integer, Integer>, ArrayList<Long>> e : degree_pairs.entrySet()) {
                    if (e.getKey().getValue() == 1 || e.getKey().getKey() == 1) {
                        sum_single += e.getValue().size();
                        for (Long rel_id : e.getValue()) {
                            Relationship r = graphdb.getRelationshipById(rel_id);
                            deleteRelationshipFromDB(r, deletedNodes);
                            deletedEdges.add(r.getId());
                        }
                    }
                }
                tx.success();
            } catch (NotFoundException e) {
                System.out.println("no property found exception ");
                System.exit(0);
            }
            getDegreePairs();
            System.out.println("delete single : pre:" + pre_node_num + " " + pre_edge_num + " " + pre_degree_num + " " +
                    "single_edges:" + sum_single + " " +
                    "post:" + neo4j.getNumberofNodes() + " " + neo4j.getNumberofEdges() + " " +
                    "dgr_paris:" + degree_pairs.size());
        }
        return deletedEdges;
    }


    private boolean hasSingletonPairs() {
        for (Map.Entry<Pair<Integer, Integer>, ArrayList<Long>> e : this.degree_pairs.entrySet()) {
            if (e.getKey().getValue() == 1 || e.getKey().getKey() == 1) {
                return true;
            }
        }
        return false;
    }

    private void deleteRelationshipFromDB(Relationship r, HashSet<Long> deletedNodes) {
        try (Transaction tx = this.neo4j.graphDB.beginTx()) {
            r.delete();
            Node sNode = r.getStartNode();
            Node eNode = r.getEndNode();

            /**If one node become isolated, remove it from the graph database.**/
            if (sNode.getDegree(Direction.BOTH) == 0) {
                deletedNodes.add(sNode.getId());
                sNode.delete();
            }
            if (eNode.getDegree(Direction.BOTH) == 0) {
                deletedNodes.add(eNode.getId());
                eNode.delete();
            }
            tx.success();
        }
    }

//    private void updateNeb4jConnectorInDynamicForests() {
//        for (Map.Entry<Integer, SpanningForests> sp_forests_e : this.dforests.dforests.entrySet()) {
//            for (SpanningTree sp_tree : sp_forests_e.getValue().trees) {
//                sp_tree.neo4j = this.neo4j;
//            }
//        }
//    }

    public HashMap<Long, NodeCoefficient> getNodesCoefficientList() {

        HashMap<Long, NodeCoefficient> nodes_coefficient_list = new HashMap<>();
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
                double coefficient2 = 1.0 * num_connected_one_with_sec_neighbors / (neighbors.size() * (neighbors.size() - 1));
                double coefficient3 = 1.0 * num_connected_sec_neighbors / (second_neighbors.size() * (second_neighbors.size() - 1));

                coefficient1 = Double.isNaN(coefficient1) ? 1 : 1 - coefficient1;
                coefficient2 = Double.isNaN(coefficient2) ? 1 : 1 - coefficient2; // the lower the better.
                coefficient3 = Double.isNaN(coefficient3) ? 1 : 1 - coefficient3;

//                if (coefficient1 != -1 && coefficient2 != -1 && coefficient3 != -1) {
//                    System.out.println(c_node.getId() + "  " + c_node.getDegree(Direction.BOTH) + "  " + coefficient1 + "  " + num_connected_one_with_sec_neighbors + " "
//                            + neighbors.size() + " " + second_neighbors.size() + " " + coefficient3 + "   " + coefficient2);
//                }
                NodeCoefficient n_coff = new NodeCoefficient(coefficient2, neighbors.size(), second_neighbors.size());
                nodes_coefficient_list.put(c_node.getId(), n_coff);
            }

            tx.success();
        }

        return nodes_coefficient_list;
    }

    private int getNumberOfConnectedNeighbors
            (HashSet<Node> neighbors, HashSet<Node> second_neighbors, Neo4jDB neo4j) {
        int connections = 0;

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
                            connections++;
                        }
                    }
                }

            }
        }
        return connections;
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

    private void createIndexFolder() {
        String folder = "/home/gqxwolf/mydata/projectData/BackBone/indexes/" + this.folder_name + "/";

        File idx_folder = new File(folder);
        try {
            if (idx_folder.exists()) {
                System.out.println("delete the folder : " + folder);
                FileUtils.deleteDirectory(idx_folder);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        idx_folder.mkdirs();
    }

    /***
     * Build the index for each layer based on the deletedEdges_layers information
     *
     * Two cases:
     * 1) The max level graph is empty. After deleting the edges, there is no remaining nodes and edges at the max level graph.
     * 2) The max level graph is non-empty. the max level graph only contains the edges that degree pair either <2,x> or <2,x>
     */
    private void indexBuild() {
        System.out.println("============== index building process  ==========================");
        long overallIndex = 0;
        int maxlevel = this.deletedEdges_layer.size();
        System.out.println("There " + maxlevel + " layer indexes");
        for (int l = 0; l <= maxlevel; l++) {

            int level = l;

            int nextlevel = level + 1;
            HashSet<Long> remind_nodes = new HashSet<>(); // the nodes that is remained in next level.

            if (l != maxlevel) {
                remind_nodes = getNodeListAtLevel(nextlevel);
            }

            HashSet<Long> de = null;
            if (l != maxlevel) {
                de = deletedEdges_layer.get(level);
            }

            String graph_db_folder = this.base_db_name + "_Level" + level;
            Neo4jDB neo4j_level = new Neo4jDB(graph_db_folder);
            neo4j_level.startDB(true);
            GraphDatabaseService graphdb_level = neo4j_level.graphDB;
            long levelcn = neo4j_level.getNumberofNodes();

            boolean have_nodes_last_graph = false;

            if (remind_nodes.size() == 0 && l == maxlevel - 1) {
                System.out.println("The last graph is empty, current level " + l + " needs to build the index between each pair-wise nodes, but so far does not !!!");
                neo4j_level.closeDB();
                return;
            } else if (neo4j_level.getNumberofNodes() != 0 && l == maxlevel) {
                have_nodes_last_graph = true;
            }


            //the folder of the indexes of level l
            String sub_folder_str = "/home/gqxwolf/mydata/projectData/BackBone/indexes/" + this.folder_name + "/level" + level;

            File sub_folder_f = new File(sub_folder_str);
            if (sub_folder_f.exists()) {
                sub_folder_f.delete();
            }
            sub_folder_f.mkdirs();

            System.out.println(neo4j_level.DB_PATH + "   deleted edges:" + (de == null ? -1 : de.size()) + "     # of edges:" + neo4j_level.getNumberofEdges() + "    # of nodes : " + neo4j_level.getNumberofNodes());
            System.out.println(level + " " + nextlevel + " " + maxlevel + "  size of remind nodes " + remind_nodes.size());

            if (have_nodes_last_graph && l == maxlevel) {
                System.out.println("The last level has nodes, but do not build index for all-pair-wise nodes !!!!!! ");
                neo4j_level.closeDB();
                return;
            }

            long numIndex = 0;
            long sizeOverallSkyline = 0;
            try (Transaction tx = graphdb_level.beginTx()) {
                ResourceIterable<Node> allnodes_iteratable = graphdb_level.getAllNodes();
                ResourceIterator<Node> allnodes_iter = allnodes_iteratable.iterator();

                BufferedWriter writer;

                int counter = 0;
                while (allnodes_iter.hasNext()) {
                    long one_iter_start = System.nanoTime();

                    HashMap<Long, myQueueNode> tmpStoreNodes = new HashMap();
                    Node node = allnodes_iter.next();

                    long nodeID = node.getId();

                    myQueueNode snode = new myQueueNode(nodeID, neo4j_level);
                    myNodePriorityQueue mqueue = new myNodePriorityQueue();
                    tmpStoreNodes.put(snode.id, snode);
                    mqueue.add(snode);
                    while (!mqueue.isEmpty()) {
                        myQueueNode v = mqueue.pop();
                        for (int i = 0; i < v.skyPaths.size(); i++) {
                            path p = v.skyPaths.get(i);
                            if (!p.expaned) {
                                p.expaned = true;

                                ArrayList<path> new_paths;
                                if (de != null) {
                                    new_paths = p.expand(neo4j_level, de);
                                } else {
                                    new_paths = p.expand(neo4j_level);
                                }

                                for (path np : new_paths) {
                                    myQueueNode next_n;
                                    if (tmpStoreNodes.containsKey(np.endNode)) {
                                        next_n = tmpStoreNodes.get(np.endNode);
                                    } else {
                                        next_n = new myQueueNode(snode, np.endNode, neo4j_level);
                                        tmpStoreNodes.put(next_n.id, next_n);
                                    }

                                    if (next_n.addToSkyline(np) && !next_n.inqueue) {
                                        mqueue.add(next_n);
                                        next_n.inqueue = true;
                                    }
                                }
                            }
                        }
                    }


                    int sum = 0;
                    for (Map.Entry<Long, myQueueNode> e : tmpStoreNodes.entrySet()) {
                        ArrayList<path> sk = e.getValue().skyPaths;
                        //remove the index of the self connection that node only has one skyline path and the skyline path is to itself
                        if (!(sk.size() == 1 && sk.get(0).costs[0] == 0 && sk.get(0).costs[1] == 0 && sk.get(0).costs[2] == 0)) {
                            for (path p : sk) {
                                if (l != maxlevel && remind_nodes.size() != 0 && remind_nodes.contains(p.endNode)) { // not the max_level graph, have reminding nodes on next level
                                    sum++;
                                } else if (l != maxlevel && remind_nodes.size() == 0) { // the max_level-1 level graph and the next level (the max_level) graph is empty, find the skyline paths between all the nodes
                                    sum++;
                                } else if (l == maxlevel && have_nodes_last_graph) { // the max_level graph is not empty, find the skyline paths between all the nodes.
                                    sum++;
                                }
                            }
                        }
                    }

                    if (counter % 500 == 0) {
                        System.out.println(counter + "/" + levelcn + "...........................");
                    }
//                    System.out.println(counter + "   " + nodeID + "    ......... (" + ((System.nanoTime() - one_iter_start) / 1000000.0) + " ms )" + "     size of the skylines " + sum);
                    counter++;

                    sizeOverallSkyline += sum;

                    if (sum != 0) {
                        numIndex++;
                        /**clean the built index file*/
                        File idx_file = new File(sub_folder_str + "/" + nodeID + ".idx");
                        if (idx_file.exists()) {
                            idx_file.delete();
                        }

                        writer = new BufferedWriter(new FileWriter(idx_file.getAbsolutePath()));
                        for (Map.Entry<Long, myQueueNode> e : tmpStoreNodes.entrySet()) {
                            long nodeid = e.getKey();
                            myQueueNode node_obj = e.getValue();
                            ArrayList<path> skys = node_obj.skyPaths;
                            for (path p : skys) {
                                /** the end node of path is a highway, the node is still appear in next level, also, the path is not a dummy path of source node **/
                                if (p.endNode != nodeID) {
                                    if (l != maxlevel && remind_nodes.size() != 0 && remind_nodes.contains(p.endNode)) { // not the max_level graph, have reminding nodes on next level
                                        writer.write(nodeid + " " + p.costs[0] + " " + p.costs[1] + " " + p.costs[2] + "\n");
                                    } else if (l != maxlevel && remind_nodes.size() == 0) { // the max_level-1 level graph and the next level (the max_level) graph is empty, find the skyline paths between all the nodes
                                        writer.write(nodeid + " " + p.costs[0] + " " + p.costs[1] + " " + p.costs[2] + "\n");
                                    } else if (l == maxlevel && have_nodes_last_graph) { // the max_level graph is not empty, find the skyline paths between all the nodes.
                                        writer.write(nodeid + " " + p.costs[0] + " " + p.costs[1] + " " + p.costs[2] + "\n");
                                    }
                                }
                            }
                        }
                        writer.close();
                    }
                }

                overallIndex += sizeOverallSkyline;
                System.out.println(numIndex + "    " + sizeOverallSkyline);
                tx.success();
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println(overallIndex);
            neo4j_level.closeDB();
        }
    }


    private HashSet<Long> getNodeListAtLevel(int level) {
        HashSet<Long> nodeList = new HashSet<>();
        String graph_db_folder = this.base_db_name + "_Level" + level;

        String graph_db_full_folder = prop.params.get("neo4jdb") + "/" + this.base_db_name + "_Level" + level;
        if (!new File(graph_db_full_folder).exists()) {
            return nodeList;
        }

        Neo4jDB neo4j_level = new Neo4jDB(graph_db_folder);
        neo4j_level.startDB(true);
        GraphDatabaseService graphdb_level = neo4j_level.graphDB;

        try (Transaction tx = graphdb_level.beginTx()) {
            ResourceIterable<Node> allnodes_iteratable = graphdb_level.getAllNodes();
            ResourceIterator<Node> allnodes_iter = allnodes_iteratable.iterator();
            while (allnodes_iter.hasNext()) {
                Node node = allnodes_iter.next();
                nodeList.add(node.getId());
            }
            tx.success();
        }
        neo4j_level.closeDB();
        return nodeList;
    }


    private HashSet<Long> getNodeListAtLevel() {
        HashSet<Long> nodeList = new HashSet<>();

        GraphDatabaseService graphdb_level = this.neo4j.graphDB;

        try (Transaction tx = graphdb_level.beginTx()) {
            ResourceIterable<Node> allnodes_iteratable = graphdb_level.getAllNodes();
            ResourceIterator<Node> allnodes_iter = allnodes_iteratable.iterator();
            while (allnodes_iter.hasNext()) {
                Node node = allnodes_iter.next();
                nodeList.add(node.getId());
            }
            tx.success();
        }
        return nodeList;
    }

    /**
     * Find the list of node that is remained in current layer, by given a specific cluster
     *
     * @param node_list the node list in a cluster
     * @return the list of node that is remained in current layer.
     */
    private HashSet<Long> getNodeListAtLevel(HashSet<Long> node_list) {
        HashSet<Long> result = new HashSet<>();
//        long src_node_id = -1;
//        int i = 1;
        try (Transaction tx = neo4j.graphDB.beginTx()) {
            for (long node_id_in_list : node_list) {
//                src_node_id = node_id_in_list;
                try {
                    long node_id = neo4j.graphDB.getRelationshipById(node_id_in_list).getId();
//                    System.out.println(i++ + "  " + node_id);
                    result.add(node_id);
                } catch (NotFoundException nofundexpection) {
//                    System.err.println("Can not find the node (" + i++ + ")" + src_node_id + " in the cluster in " + neo4j.graphDB);
                }
            }
            tx.success();
        }

        return result;
    }

    private Pair<Integer, Integer> updateThreshold(double percentage) {

        ArrayList<Pair<Integer, Integer>> t_degree_pair = new ArrayList<>(this.degree_pairs.keySet());

        if (t_degree_pair.isEmpty()) {
            return null;
        }
        System.out.println("Find updated threshold:");
        int max = (int) (this.numberOfEdges * percentage);

        //get the last degree pair
        if (max > this.degree_pairs_sum) {
            return t_degree_pair.get(t_degree_pair.size() - 1);
        }
        System.out.println("number of edges:" + this.numberOfEdges + "   percentage:" + max + "   number of nodes :" + cn + "   degree pair sum( number of edges ):" + degree_pairs_sum + "   Is the Max is less than the number of degree pair ? " + (max < this.degree_pairs_sum));

//        for (Map.Entry<Pair<Integer, Integer>, ArrayList<Long>> d : degree_pairs.entrySet()) {
//            System.out.println(d.getKey() + "  :   " + d.getValue().size());
//        }

        int t_num = 0;
        int idx;

        for (idx = 0; idx < t_degree_pair.size(); idx++) {
            Pair<Integer, Integer> key = t_degree_pair.get(idx);

//            if (key.getKey() == 2 || key.getValue() == 2) {
//                continue;
//            }

            t_num += this.degree_pairs.get(key).size();
            if (t_num >= max) {
                break;
            }
        }

        //idx is the index of the first degree pair which summation number is greater than |E|*p
        if (0 <= idx && idx < t_degree_pair.size()) {
            return t_degree_pair.get(idx);
        } else if (idx >= t_degree_pair.size()) {
            return t_degree_pair.get(t_degree_pair.size() - 1); //remove all the degree pair whose value is not equal to <2,x> or <x,2>
        }

        return null;
    }

    public boolean addToSkyline(ArrayList<double[]> skyline_costs, double[] costs) {
        int i = 0;

        if (skyline_costs.isEmpty()) {
            skyline_costs.add(costs);
            return true;
        } else {
            boolean can_insert_np = true;
            for (; i < skyline_costs.size(); ) {
                if (checkDominated(skyline_costs.get(i), costs)) {
                    can_insert_np = false;
                    break;
                } else {
                    if (checkDominated(costs, skyline_costs.get(i))) {
                        skyline_costs.remove(i);
                    } else {
                        i++;
                    }
                }
            }
            if (can_insert_np) {
                skyline_costs.add(costs);
                return true;
            }
        }
        return false;
    }

    private boolean checkDominated(double[] costs, double[] estimatedCosts) {
        for (int i = 0; i < costs.length; i++) {
            if (costs[i] > estimatedCosts[i]) {
                return false;
            }
        }
        return true;
    }
}
