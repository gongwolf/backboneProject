package v5LinkedList;

import Neo4jTools.Neo4jDB;
import configurations.ProgramProperty;
import javafx.util.Pair;
import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.*;
import org.neo4j.register.Register;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class IndexPathBuild {
    private int graphsize = 10000;
    private int degree = 4;
    private int dimension = 3;
    private double samenode_t = 2.844;
    private Neo4jDB neo4j;
    private GraphDatabaseService graphdb;
    private long cn; //number of graph nodes
    private long numberOfEdges; // number of edges ;
    private DynamicForests dforests;
    private double percentage;
    public ProgramProperty prop = new ProgramProperty();
    public String city_name;
    public String base_db_name = "sub_ny_USA";
//    public String base_db_name = "col_USA";


    //Pair <sid_degree,did_degree> -> list of the relationship id that the degrees of the start node and end node are the response given pair of key
    TreeMap<Pair<Integer, Integer>, ArrayList<Long>> degree_pairs = new TreeMap(new PairComparator());

    //deleted edges record the relationship deleted in each layer, the index of each layer is based on the expansion on previous level graph
    ArrayList<HashSet<Long>> deletedEdges_layer = new ArrayList<>();
    private int degree_pairs_sum;

    public static void main(String args[]) throws CloneNotSupportedException {
        long start = System.currentTimeMillis();
        IndexPathBuild index = new IndexPathBuild();
        index.percentage = 0.02;
        index.city_name = "ny_USA";
        index.build();
        long end = System.currentTimeMillis();
        System.out.println("Total Running time " + (end - start) * 1.0 / 1000 + "s  ~~~~~ ");
    }

    private void build() throws CloneNotSupportedException {
        initLevel();
        construction();
        createIndexFolder();
        indexBuild();
    }

    private void initLevel() {
//        String sub_db_name = "col_USA_Level0";
        String sub_db_name = "sub_ny_USA_Level0";
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


    private void construction() throws CloneNotSupportedException {
        int currentLevel = 0;
        Pair<Integer, Integer> threshold = updateThreshold(percentage);
        int t_threshold = threshold.getKey() + threshold.getValue();
        boolean nodes_deleted = false;

        do {
            int upperlevel = currentLevel + 1;
            System.out.println("===============  level:" + upperlevel + " ==============");
            System.out.println("threshold:" + threshold.getKey() + "+" + threshold.getValue() + " = " + t_threshold);

            //copy db from previous level
            copyToHigherDB(currentLevel, upperlevel);

            //handle the degrees pairs
            nodes_deleted = handleUpperLevelGraph(upperlevel, threshold, t_threshold);

//            threshold = updateThreshold(threshold,t_threshold);
            threshold = updateThreshold(percentage);
            if (threshold != null) {
                t_threshold = threshold.getKey() + threshold.getValue();
                currentLevel = upperlevel;
            }

        } while (nodes_deleted);
//        } while (cn != 0 && threshold != null);

        System.out.println("finish the index finding, the current level is " + currentLevel + "  with number of nodes :" + cn);

        int lastLevel = currentLevel;
        dealwithLastLayer(lastLevel);
    }

    private void dealwithLastLayer(int lastLevel) {
        String sub_db_name = this.base_db_name + "_Level" + lastLevel;
        neo4j = new Neo4jDB(sub_db_name);
        neo4j.startDB(false);
        graphdb = neo4j.graphDB;
        long pre_n = neo4j.getNumberofNodes();
        long pre_e = neo4j.getNumberofEdges();
        System.out.println("deal with the last level " + lastLevel + " graph at " + neo4j.DB_PATH + "  " + pre_n + " nodes and " + pre_e + " edges");
        neo4j.closeDB();
        System.out.println("**********************************************************************************************************************");
        int currentlevel = lastLevel;
        boolean deleted = false;
        do {
            if (currentlevel != lastLevel) {
                int previouslevel = currentlevel;
                currentlevel = currentlevel + 1;
                copyToHigherDB(previouslevel, currentlevel);
            }

            deleted = handleUpperLevelGraphWithGivenThreshold(currentlevel);
            break;
        } while (deleted);


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

        for (Map.Entry<Pair<Integer, Integer>, ArrayList<Long>> d : degree_pairs.entrySet()) {
            System.out.println(d.getKey() + "  :   " + d.getValue().size());
        }

        int t_num = 0;
        int idx;

        for (idx = 0; idx < t_degree_pair.size(); idx++) {
            Pair<Integer, Integer> key = t_degree_pair.get(idx);

            if (key.getKey() == 2 || key.getValue() == 2) {
                continue;
            }

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


    private boolean handleUpperLevelGraphWithGivenThreshold(int currentLevel) {
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


        updateNeb4jConnectorInDynamicForests();
        System.out.println("Updated the neo4j database object ................");
        getDegreePairs();
        ArrayList<Pair<Integer, Integer>> threshold_list = new ArrayList<>();
        Pair<Integer, Integer> threshold = this.degree_pairs.keySet().iterator().next();
        threshold_list.add(threshold);
        System.out.println("#####");


        boolean deleted = removeLowerDegreePairEdgesByList(threshold_list, deletedNodes, deletedEdges);

        System.out.println("Removing the edges in level " + currentLevel + "  with degree threshold list : " + threshold_list);
        long post_n = neo4j.getNumberofNodes();
        long post_e = neo4j.getNumberofEdges();
        System.out.println("~~~~~~~~~~~~~ pre:" + pre_n + " " + pre_e + "  post:" + post_n + " " + post_e + "   # of deleted Edges:" + deletedEdges.size());
        String textFilePath = prefix + "busline_sub_graph_NY/non-single/level" + currentLevel + "/";
        neo4j.saveGraphToTextFormation(textFilePath);

        getDegreePairs();
        deletedEdges.addAll(removeSingletonEdgesInForests(deletedNodes));
        long numberOfNodes = neo4j.getNumberofNodes();
        post_n = neo4j.getNumberofNodes();
        post_e = neo4j.getNumberofEdges();
//
        textFilePath = prefix + "busline_sub_graph_NY/level" + currentLevel + "/";
        neo4j.saveGraphToTextFormation(textFilePath);
//
        System.out.println(" ~~~~~~~~~~~~~ pre:" + pre_n + " " + pre_e + "  post:" + post_n + " " + post_e + "   # of deleted Edges:" + deletedEdges.size());
        System.out.println("Add the deleted edges of the level " + currentLevel + "  to the deletedEdges_layer structure. ");
        neo4j.closeDB();

        cn = numberOfNodes;

        if (cn == 0) {
            deleted = false; //no edge can be removed anymore.
        }

        if (deleted) {
            this.deletedEdges_layer.add(deletedEdges);
        }

        return deleted;
    }

    private boolean handleUpperLevelGraph(int currentLevel, Pair<Integer, Integer> threshold_p, int threshold_t) {
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
            threshold_p = updateThreshold(percentage);
            this.dforests.createBase(sptree_base);
            System.out.println("Finish finding the level 0 spanning tree..........");
        } else {
            updateNeb4jConnectorInDynamicForests();
            System.out.println("Updated the neo4j database object ................");
        }

        System.out.println("#####");


        boolean deleted = removeLowerDegreePairEdgesByThreshold(threshold_p, threshold_t, deletedNodes, deletedEdges);

        System.out.println("Removing the edges in level " + currentLevel + "  with degree threshold  : " + threshold_p);
        long post_n = neo4j.getNumberofNodes();
        long post_e = neo4j.getNumberofEdges();
        System.out.println("~~~~~~~~~~~~~ pre:" + pre_n + " " + pre_e + "  post:" + post_n + " " + post_e + "   # of deleted Edges:" + deletedEdges.size());
        String textFilePath = prefix + "busline_sub_graph_NY/non-single/level" + currentLevel + "/";
        neo4j.saveGraphToTextFormation(textFilePath);

        getDegreePairs();
        deletedEdges.addAll(removeSingletonEdgesInForests(deletedNodes));
        long numberOfNodes = neo4j.getNumberofNodes();
        post_n = neo4j.getNumberofNodes();
        post_e = neo4j.getNumberofEdges();
//
        textFilePath = prefix + "busline_sub_graph_NY/level" + currentLevel + "/";
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


    private boolean removeLowerDegreePairEdgesByList(ArrayList<Pair<Integer, Integer>> threshold_list, HashSet<Long> deletedNodes, HashSet<Long> deletedEdges) {
        getDegreePairs();

        boolean deleted = false;
        for (Pair<Integer, Integer> t : threshold_list) {
            HashSet<Long> deleted_rels = new HashSet<>(this.degree_pairs.get(t));
            try (Transaction tx = graphdb.beginTx()) {
                for (long rel : deleted_rels) {
                    Relationship r = graphdb.getRelationshipById(rel);
                    boolean flag = deleteEdge(r, deletedNodes);
//                    System.out.println("remove ...... " + rel + "  " + r + "   " + flag + "       " + dforests.isTreeEdge(10263l));
//                    dforests.isInTheTree(10263l);

                    if (flag) {
                        deletedEdges.add(r.getId());
                        if (!deleted) {
                            deleted = true;
                        }
                    }
                }
                tx.success();

            }
            System.out.println("Finished the remove of the edge by using the threshold " + t + " !!!!!!!!!!!!!!!!!!!!!!");
        }


        return deleted;
    }

    private boolean removeLowerDegreePairEdgesByThreshold(Pair<Integer, Integer> threshold_p, int threshold_t, HashSet<Long> deletedNodes, HashSet<Long> deletedEdges) {
        boolean deleted = false;
        for (Map.Entry<Pair<Integer, Integer>, ArrayList<Long>> dp : this.degree_pairs.entrySet()) {
            int sd = dp.getKey().getKey(); // the first number of the degree pair
            int ed = dp.getKey().getValue(); // the second number of the degree pair
            int ct = sd + ed; //summation of the degree pair.

            if (sd == 2 || ed == 2) {
                continue;
            }

            /**
             * Four case pair of degree needs to be deleted
             * 1) the summation of the degree pair (ct) is less than the summation of the given threshold degree pair (threshold_t).
             * 2) if threshold_t is equal to ct, the first number of the degree pair is less than the first number of the the given degree pair
             * 3) the given degree pair is equal to the degree pair dp.
             */

            if ((ct < threshold_t) || (threshold_t == ct && sd < threshold_p.getKey()) || (sd == threshold_p.getKey() && ed == threshold_p.getValue()) || (ed == threshold_p.getKey() && sd == threshold_p.getValue())) {
                try (Transaction tx = graphdb.beginTx()) {
                    HashSet<Long> deleted_rels = new HashSet<>(dp.getValue());
//                    Collections.shuffle(deleted_rels); //Shuffle the deleted edges

                    for (long rel : deleted_rels) {
                        Relationship r = graphdb.getRelationshipById(rel);

                        boolean flag = deleteEdge(r, deletedNodes);


                        if (flag) {
                            deletedEdges.add(r.getId());
                            if (!deleted) {
                                deleted = true;
                            }
                        }
                    }
                    tx.success();
                }
            }
        }

        System.out.println("Finished the remove of the edge by using the threshold " + threshold_p + " !!!!!!!!!!!!!!!!!!!!!!");
        return deleted;
    }

    private boolean deleteEdge(Relationship r, HashSet<Long> deletedNodes) {
        boolean canBeDeleted;
        if (dforests.isTreeEdge(r.getId())) {
            canBeDeleted = this.dforests.deleteEdge(r);
            if (canBeDeleted) {
                deleteRelationshipFromDB(r, deletedNodes);
            }
        } else {
            deleteRelationshipFromDB(r, deletedNodes);
            canBeDeleted = true;
        }

        return canBeDeleted;
    }

    private void updateNeb4jConnectorInDynamicForests() {
        for (Map.Entry<Integer, SpanningForests> sp_forests_e : this.dforests.dforests.entrySet()) {
            for (SpanningTree sp_tree : sp_forests_e.getValue().trees) {
                sp_tree.neo4j = this.neo4j;
            }
        }
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


    private void createIndexFolder() {
        String folder = "/home/gqxwolf/mydata/projectData/BackBone/indexes/busline_sub_graph_NY/";

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

        System.out.println("------------------------------------- deletedEdges_layer");
        int level = 0;
        for (HashSet<Long> de_layer : deletedEdges_layer) {
            System.out.println((level++) + "    " + de_layer.size());
        }
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

            boolean have_nodes_last_graph = false;

            if (remind_nodes.size() == 0 && l == maxlevel - 1) {
                System.out.println("The last graph is empty, current level " + l+" needs to build the index between each pair-wise nodes, but so far does not !!!");
                neo4j_level.closeDB();
                return;
            } else if (neo4j_level.getNumberofNodes() != 0 && l == maxlevel) {
                have_nodes_last_graph = true;
            }


            //the folder of the indexes of level l
            String sub_folder_str = "/home/gqxwolf/mydata/projectData/BackBone/indexes/busline_sub_graph_NY/level" + level;

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

                int counter = 1;
                while (allnodes_iter.hasNext()) {
                    long one_iter_start = System.nanoTime();

                    HashMap<Long, myNode> tmpStoreNodes = new HashMap();
                    Node node = allnodes_iter.next();

                    long nodeID = node.getId();

                    myNode snode = new myNode(nodeID, neo4j_level);
                    myNodePriorityQueue mqueue = new myNodePriorityQueue();
                    tmpStoreNodes.put(snode.id, snode);
                    mqueue.add(snode);
                    while (!mqueue.isEmpty()) {
                        myNode v = mqueue.pop();
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
                                    myNode next_n;
                                    if (tmpStoreNodes.containsKey(np.endNode)) {
                                        next_n = tmpStoreNodes.get(np.endNode);
                                    } else {
                                        next_n = new myNode(snode, np.endNode, neo4j_level);
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
                    for (Map.Entry<Long, myNode> e : tmpStoreNodes.entrySet()) {
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

//                    if (level == maxlevel - 2 || level == maxlevel - 1 || level == maxlevel) {
//                        long one_iter_rt = System.nanoTime() - one_iter_start;
//                        System.out.println(counter + "   " + nodeID + "    ......... (" + one_iter_rt / 1000000.0 + " ms )" + "  size of the skylines " + sum + "   (" + (l != maxlevel) + "  " + (remind_nodes.size() != 0) + ")");
//                        counter++;
//                    }

                    sizeOverallSkyline += sum;

                    if (sum != 0) {
                        numIndex++;
                        /**clean the built index file*/
                        File idx_file = new File(sub_folder_str + "/" + nodeID + ".idx");
                        if (idx_file.exists()) {
                            idx_file.delete();
                        }

                        writer = new BufferedWriter(new FileWriter(idx_file.getAbsolutePath()));
                        for (Map.Entry<Long, myNode> e : tmpStoreNodes.entrySet()) {
                            long nodeid = e.getKey();
                            myNode node_obj = e.getValue();
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

}


//a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater than the second.
class PairComparator implements Comparator<Pair<Integer, Integer>> {
    @Override
    public int compare(Pair<Integer, Integer> o1, Pair<Integer, Integer> o2) {
        if ((o1.getKey() + o1.getValue()) - (o2.getKey() + o2.getValue()) != 0) {
            return (o1.getKey() + o1.getValue()) - (o2.getKey() + o2.getValue());
        } else {
            return o1.getKey() - o2.getKey();
        }//sort in descending order
    }
}
