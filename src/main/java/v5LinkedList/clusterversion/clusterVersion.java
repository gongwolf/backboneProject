package v5LinkedList.clusterversion;

import Neo4jTools.Line;
import Neo4jTools.Neo4jDB;
import configurations.ProgramProperty;
import javafx.util.Pair;
import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.*;
import utilities.CollectionOperations;
import v5LinkedList.DynamicForests;
import v5LinkedList.PairComparator;
import v5LinkedList.SpanningForests;
import v5LinkedList.SpanningTree;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class clusterVersion {

    private Neo4jDB neo4j;
    private GraphDatabaseService graphdb;
    private long cn; //number of graph nodes
    private long numberOfEdges; // number of edges ;
    private DynamicForests dforests;
    private double percentage;
    public ProgramProperty prop = new ProgramProperty();
    public String city_name;
    //    public String base_db_name = "sub_ny_USA";
    //    public String base_db_name = "col_USA";
    public String base_db_name = "sub_ny_USA";
    public String folder_name = "busline_sub_graph_NY";

    //Pair <sid_degree,did_degree> -> list of the relationship id that the degrees of the start node and end node are the response given pair of key
    TreeMap<Pair<Integer, Integer>, ArrayList<Long>> degree_pairs = new TreeMap(new PairComparator());

    //deleted edges record the relationship deleted in each layer, the index of each layer is based on the expansion on previous level graph
    ArrayList<HashSet<Long>> deletedEdges_layer = new ArrayList<>();

    private int degree_pairs_sum;
    private HashSet<Long> visited_nodes;


    public static void main(String args[]) throws CloneNotSupportedException {
        long start = System.currentTimeMillis();
        clusterVersion index = new clusterVersion();
        index.percentage = 0.02;
        index.city_name = "ny_USA";
        index.build();
        long end = System.currentTimeMillis();
        System.out.println("Total Running time " + (end - start) * 1.0 / 1000 + "s  ~~~~~ ");
    }


    private void build() throws CloneNotSupportedException {
        initLevel();
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

        } while (nodes_deleted);
//        } while (cn != 0 && threshold != null);

        System.out.println("finish the index finding, the current level is " + currentLevel + "  with number of nodes :" + cn);
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

        if (currentLevel == 1) {
            getDegreePairs();
            deletedEdges.addAll(removeSingletonEdges(neo4j, deletedEdges));
            dforests = new DynamicForests();
            SpanningTree sptree_base = new SpanningTree(neo4j, true);
            System.out.println("=======================================");
            sptree_base.EulerTourStringWiki(0);
            this.dforests.createBase(sptree_base);
            System.out.println("Finish finding the level 0 spanning tree..........");
        } else {
            updateNeb4jConnectorInDynamicForests();
            System.out.println("Updated the neo4j database object ................");
        }

        System.out.println("#####");


        boolean deleted = removeLowerDegreePairEdgesByThreshold(deletedNodes, deletedEdges);

        System.out.println("Removing the edges in level " + currentLevel + "  with degree threshold  : ");
        long post_n = neo4j.getNumberofNodes();
        long post_e = neo4j.getNumberofEdges();
        System.out.println("~~~~~~~~~~~~~ pre:" + pre_n + " " + pre_e + "  post:" + post_n + " " + post_e + "   # of deleted Edges:" + deletedEdges.size());
        String textFilePath = prefix + folder_name + "/non-single/level" + currentLevel + "/";
        neo4j.saveGraphToTextFormation(textFilePath);

        getDegreePairs();
        deletedEdges.addAll(removeSingletonEdgesInForests(deletedNodes));
        long numberOfNodes = neo4j.getNumberofNodes();
        post_n = neo4j.getNumberofNodes();
        post_e = neo4j.getNumberofEdges();
//
        textFilePath = prefix + folder_name + "/level" + currentLevel + "/";
        neo4j.saveGraphToTextFormation(textFilePath);
//
        System.out.println(" ~~~~~~~~~~~~~ pre:" + pre_n + " " + pre_e + "  post:" + post_n + " " + post_e + "   # of deleted Edges:" + deletedEdges.size());
        System.out.println("Add the deleted edges of the level " + currentLevel + "  to the deletedEdges_layer structure. ");
        neo4j.closeDB();

        cn = numberOfNodes;

        if (cn == 0) {
            deleted = false;
        }

        if (deleted) {
            this.deletedEdges_layer.add(deletedEdges);
        }
        return deleted;
    }

    private boolean removeLowerDegreePairEdgesByThreshold(HashSet<Long> deletedNodes, HashSet<Long> deletedEdges) {

        HashMap<Long, myNode> visited_nodes = new HashMap<>();

        NodeClusters node_clusters = new NodeClusters();

        HashMap<Long, Double> node_coefficient_list = getNodesCoefficientList();
        HashMap<Long, Double> sorted_coefficient = CollectionOperations.sortHashMapByValue(node_coefficient_list);

//        sorted_coefficient.forEach((k, v) -> System.out.println(k + "  " + v));

        System.out.println(sorted_coefficient.size());
        for (Map.Entry<Long, Double> node_coeff : sorted_coefficient.entrySet()) {

            long node_id = node_coeff.getKey();

            if (node_coeff.getValue() == 1) {
                node_clusters.clusters.get(0).addToCluster(node_id);
            }

            if (visited_nodes.containsKey(node_id)) {
                continue;
            }

            try (Transaction tx = this.neo4j.graphDB.beginTx()) {

                double coefficient = sorted_coefficient.get(node_id);

                NodeCluster cluster = new NodeCluster(node_clusters.getNextClusterID());

                myClusterQueue queue = new myClusterQueue();

                myNode m_node = new myNode(node_id, graphdb.getNodeById(node_id), coefficient);

                queue.add(m_node);
                visited_nodes.put(node_id, m_node);
                cluster.addToCluster(m_node.id);

                while (!queue.isEmpty()) {
                    myNode n = queue.pop();
                    ArrayList<Node> neighbors = neo4j.getNeighborsNodeList(n.id);

                    boolean can_add_to_queue = !cluster.oversize();
//                    boolean can_add_to_queue = true;

                    for (Node neighbor_node : neighbors) {

                        long n_node_id = neighbor_node.getId();
                        myNode next_node;

                        if (!visited_nodes.containsKey(n_node_id)) {
                            if (can_add_to_queue) {
                                next_node = new myNode(n_node_id, neighbor_node, sorted_coefficient.get(n_node_id));
                                cluster.addToCluster(n_node_id);
                                visited_nodes.put(n_node_id, next_node);

                                if (sorted_coefficient.get(n_node_id) != 1) {
                                    queue.add(next_node);
                                }

                            }


                        } else if (visited_nodes.containsKey(n_node_id) && node_clusters.clusters.get(0).node_list.contains(n_node_id)) {
                            next_node = visited_nodes.get(n_node_id);
                            node_clusters.clusters.get(0).node_list.remove(n_node_id);
                            cluster.addToCluster(n_node_id);

                            if (sorted_coefficient.get(n_node_id) != 1) {
                                queue.add(next_node);
                            }
                        }
                    }
                }

                cluster.updateBorderList(neo4j);
                node_clusters.clusters.put(cluster.cluster_id, cluster);
//                node_id = cluster.getRandomBorderNode();
                tx.success();
            }
        }

        System.out.println("found # of clusters : " + node_clusters.clusters.size());

        node_clusters.clusters.forEach((k, v) -> v.updateBorderList(neo4j));

        node_clusters.clusters.forEach((k, v) -> {
            if (v.node_list.size() >= 100) {
                System.out.println(k + "  " + v.node_list.size() + "  " + v.border_node_list.size());
            }
        });


//        final int[] i = {0};
//        node_clusters.clusters.forEach((k, v) -> {
//            if (v.node_list.size() >= 100 && k!=0) {
////                System.out.println(k + "  " + v.node_list.size() + "  " + v.getBorderList().size());
//                v.node_list.forEach(node_id-> System.out.println(node_id+" "+ i[0]));
//                i[0]++;
//            }
//        });

        return false;
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
        //initialize the data structure that store multi-layer spanning forest.
        this.dforests = new DynamicForests();
        System.out.println("Initialization: there are " + cn + " nodes and " + numberOfEdges + " edges");
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
                            int edge_level = (int) r.getProperty("level");

                            for (int l = edge_level; l >= 0; l--) {
                                int sp_idx = this.dforests.dforests.get(l).findTreeIndex(r);

                                SpanningTree sp_tree = this.dforests.dforests.get(l).trees.get(sp_idx);
                                int remove_case = sp_tree.removeSingleEdge(r.getId());

                                if (remove_case == 1) {
                                    this.dforests.dforests.get(l).trees.remove(sp_idx);
                                } else if (remove_case == 0) {
                                    System.out.println("Remove single edge error !!!!!!!!!!!!!!!!!!!!!");
                                    System.exit(0);
                                }
                            }

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

    private void updateNeb4jConnectorInDynamicForests() {
        for (Map.Entry<Integer, SpanningForests> sp_forests_e : this.dforests.dforests.entrySet()) {
            for (SpanningTree sp_tree : sp_forests_e.getValue().trees) {
                sp_tree.neo4j = this.neo4j;
            }
        }
    }

    public HashMap<Long, Double> getNodesCoefficientList() {
        HashMap<Long, Double> nodes_coefficient_list = new HashMap<>();
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

                coefficient1 = Double.isNaN(coefficient1) ? -1 : coefficient1;
                coefficient2 = Double.isNaN(coefficient2) ? -1 : coefficient2;
                coefficient3 = Double.isNaN(coefficient3) ? -1 : coefficient3;

//                if (coefficient1 != -1 && coefficient2 != -1 && coefficient3 != -1) {
//                    System.out.println(c_node.getId() + "  " + c_node.getDegree(Direction.BOTH) + "  " + coefficient1 + "  " + num_connected_one_with_sec_neighbors + " "
//                            + neighbors.size() + " " + second_neighbors.size() + " " + coefficient3 + "   " + coefficient2);
//                }

                nodes_coefficient_list.put(c_node.getId(), 1 - coefficient2);
            }

            tx.success();
        }

        return nodes_coefficient_list;
    }

    private int getNumberOfConnectedNeighbors(HashSet<Node> neighbors, HashSet<Node> second_neighbors, Neo4jDB neo4j) {
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

}
