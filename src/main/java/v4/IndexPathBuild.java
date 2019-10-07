package v4;

import Neo4jTools.Neo4jDB;
import configurations.ProgramProperty;
import javafx.util.Pair;
import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.*;
import v3.DynamicForests;
import v3.SpanningForests;
import v3.SpanningTree;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static DataStructure.STATIC.nil;

public class IndexPathBuild {

    private int graphsize = 2000;
    private int degree = 4;
    private int dimension = 3;
    private Neo4jDB neo4j;
    private GraphDatabaseService graphdb;
    private long cn; //number of graph nodes
    private long numberOfEdges; // number of edges ;
    private DynamicForests dforests;
    private double percentage = 0.1;
    public ProgramProperty prop = new ProgramProperty();


    //Pair <sid_degree,did_degree> -> list of the relationship id that the degrees of the start node and end node are the response given pair of key
    TreeMap<Pair<Integer, Integer>, ArrayList<Long>> degree_pairs = new TreeMap(new PairComparator());

    //deleted edges record the relationship deleted in each layer, the index of each layer is based on the expansion on previous level graph
    ArrayList<HashSet<Long>> deletedEdges_layer = new ArrayList<>();
    private int degree_pairs_sum;

    public static void main(String args[]) throws CloneNotSupportedException {
        long start = System.currentTimeMillis();
        IndexPathBuild index = new IndexPathBuild();
        index.build();
        long end = System.currentTimeMillis();
        System.out.println("Total Running time " + (end - start) * 1.0 / 1000 + "s  ~~~~~ ");
    }


    private void build() throws CloneNotSupportedException {
        initLevel();
        construction();
//        System.out.println(deletedEdges_layer.size());
//        long numberofedges = 0;
//        for(HashSet<Long> de:deletedEdges_layer){
//            numberofedges+= de.size();
//        }
//        System.out.println(numberofedges);
        createIndexFolder();
        indexBuild();
//        printSummurizationInformation();
//        test();
    }

    private void initLevel() {
        int currentLevel = 0;
        String sub_db_name = graphsize + "_" + degree + "_" + dimension + "_Level" + currentLevel;
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
        do {
            int upperlevel = currentLevel + 1;
            System.out.println("===============  level:" + upperlevel + " ==============");
            System.out.println("threshold:" + threshold.getKey() + "+" + threshold.getValue() + " = " + t_threshold);

            //copy db from previous level
            copyToHigherDB(currentLevel, upperlevel);
            //handle the degrees pairs
            cn = handleUpperLevelGraph(upperlevel, threshold, t_threshold);

//            threshold = updateThreshold(threshold,t_threshold);
            threshold = updateThreshold(percentage);
            if (threshold != null) {
                t_threshold = threshold.getKey() + threshold.getValue();
                currentLevel = upperlevel;
            }

        } while (cn != 0 && threshold != null);
    }

    private long handleUpperLevelGraph(int currentLevel, Pair<Integer, Integer> threshold_p, int threshold_t) {

        Hashtable<Long, ArrayList<Long>> nodesToHighWay = new Hashtable<>();

        String sub_db_name = graphsize + "_" + degree + "_" + dimension + "_Level" + currentLevel;
        neo4j = new Neo4jDB(sub_db_name);
        neo4j.startDB(false);
        graphdb = neo4j.graphDB;
        long pre_n = neo4j.getNumberofNodes();
        long pre_e = neo4j.getNumberofEdges();
        System.out.println("deal with level " + currentLevel + " graph at " + neo4j.DB_PATH);

        HashSet<Long> deletedNodes = new HashSet<>();
        // record the edges that is deleted in this layer, the index is build based on it
        HashSet<Long> deletedEdges = new HashSet<>();

        if (currentLevel == 1) {
            dforests = new DynamicForests();
            SpanningTree sptree_base = new SpanningTree(neo4j, true);
            System.out.println("=======================================");
            sptree_base.EulerTourString(0);
            deletedEdges.addAll(removeSingletonEdgesAtLevelZero(sptree_base, deletedNodes));
            getDegreePairs();
            threshold_p = updateThreshold(percentage);
            this.dforests.createBase(sptree_base);
            System.out.println("Finish finding the level 0 spanning tree..........");
        } else {
            updateNeb4jConnectorInDynamicForests();
        }


        boolean deleted = removeLowerDegreePairEdgesByThreshold(threshold_p, threshold_t, deletedNodes, deletedEdges);

        getDegreePairs();
        deletedEdges.addAll(removeSingletonEdgesInForests(deletedNodes));
        long numberOfNodes = neo4j.getNumberofNodes();
        long post_n = neo4j.getNumberofNodes();
        long post_e = neo4j.getNumberofEdges();

        System.out.println("pre:" + pre_n + " " + pre_e + "  post:" + post_n + " " + post_e + "   # of deleted Edges:" + deletedEdges.size());
        this.deletedEdges_layer.add(deletedEdges);
        neo4j.closeDB();
        return numberOfNodes;
    }


    private boolean removeLowerDegreePairEdgesByThreshold(Pair<Integer, Integer> threshold_p, int threshold_t, HashSet<Long> deletedNodes, HashSet<Long> deletedEdges) {
        boolean deleted = false;

        try (Transaction tx = graphdb.beginTx()) {
            for (Map.Entry<Pair<Integer, Integer>, ArrayList<Long>> dp : this.degree_pairs.entrySet()) {
                int sd = dp.getKey().getKey(); // the first number of the degree pair
                int ed = dp.getKey().getValue(); // the second number of the degree pair
                int ct = sd + ed; //summation of the degree pair.

                /**
                 * Four case pair of degree needs to be deleted
                 * 1) the summation of the degree pair (ct) is less than the summation of the given threshold degree pair (threshold_t).
                 * 2) if threshold_t is equal to ct, the first number of the degree pair is less than the first number of the the given degree pair
                 * 3) the given degree pair is equal to the degree pair dp.
                 */

                if ((ct < threshold_t) ||
                        (threshold_t == ct && sd < threshold_p.getKey()) ||
                        (sd == threshold_p.getKey() && ed == threshold_p.getValue()) ||
                        (ed == threshold_p.getKey() && sd == threshold_p.getValue())) {

                    for (long rel : dp.getValue()) {
                        Relationship r = graphdb.getRelationshipById(rel);

                        boolean flag = deleteEdge(r, neo4j, deletedNodes);

                        if (flag) {
                            deletedEdges.add(r.getId());

                            if (!deleted) {
                                deleted = true;
                            }
                        }


                    }
                }
            }
            tx.success();
        }

        return deleted;
    }


    private boolean deleteEdge(Relationship r, Neo4jDB neo4j, HashSet<Long> deletedNodes) {
        GraphDatabaseService graphdb = neo4j.graphDB;

        try (Transaction tx = graphdb.beginTx()) {

            int level_r = (int) r.getProperty("level");
            Relationship replacement_edge = null;

            if (dforests.isTreeEdge(r)) {
                int l_idx = level_r;
                while (l_idx >= 0) {
                    replacement_edge = dforests.replacement(r, l_idx);
                    if (null == replacement_edge) {
                        l_idx--;
                    } else {
                        break;
                    }
                }

                /** If the r can be deleted */
                if (l_idx != -1 || r.getStartNode().getDegree(Direction.BOTH) == 1 || r.getEndNode().getDegree(Direction.BOTH) == 1) {
                    updateDynamicForest(level_r, l_idx, r, replacement_edge);
                    deleteRelationshipFromDB(r, deletedNodes);
                    tx.success();
                    return true;
                }
            } else {
                deleteRelationshipFromDB(r, deletedNodes);
                tx.success();
                return true;
            }

            tx.success();
        }
        return false;
    }

    private void updateDynamicForest(int level_r, int l_idx, Relationship delete_edge, Relationship replacement_edge) {
        for (int i = level_r; i >= 0; i--) {
            if (i > l_idx) {
                dforests.dforests.get(i).deleteEdge(delete_edge);
            } else if (i <= l_idx) {
                dforests.dforests.get(i).replaceEdge(delete_edge, replacement_edge);
            }
        }
    }

    private void updateNeb4jConnectorInDynamicForests() {
        for (Map.Entry<Integer, SpanningForests> sp_forests_e : this.dforests.dforests.entrySet()) {
            for (SpanningTree sp_tree : sp_forests_e.getValue().trees) {
                sp_tree.neo4j = this.neo4j;
            }
        }
    }

    private HashSet<Long> removeSingletonEdgesAtLevelZero(SpanningTree sptree_base, HashSet<Long> deletedNodes) {
        HashSet<Long> deletedEdges = new HashSet<>();
        while (hasSingletonPairs(degree_pairs)) {
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
                            /*****
                             * 1) Delete single edge from level 0 spanning tree,
                             * 2) The order of the key will be kept the increase order.
                             * 3) It will not affect the connectivity of the graph.
                             * 4) There is only one spanning tree so far.
                             *****/
                            // the rbtree id in specific level
                            sptree_base.rbtree.delete((Integer) r.getProperty("pFirstID0"));
                            sptree_base.rbtree.delete((Integer) r.getProperty("pSecondID0"));

//                            System.out.println(" ====remove single edge at level 0  " + r);

                            sptree_base.deleteAdditionalInformationByRelationship(r);
                            deleteRelationshipFromDB(r, deletedNodes);
                            deletedEdges.add(r.getId()); // the ids of the deleted edge
                        }
                    }
                }
                tx.success();
                if (sptree_base.rbtree.root == nil) {
                    System.out.println("There is no any nodes in the level 0 spanning tree, terminate the program");
                    System.exit(0);
                }
            }
            getDegreePairs();
            this.cn = neo4j.getNumberofNodes();
            this.numberOfEdges = neo4j.getNumberofEdges();
            System.out.println("delete single at level0  : pre:" + pre_node_num + " " + pre_edge_num + " dgr_paris:" + pre_degree_num + " " +
                    "single_edges:" + sum_single + " " +
                    "post:" + neo4j.getNumberofNodes() + " " + neo4j.getNumberofEdges() + " " +
                    "dgr_paris:" + degree_pairs.size());
        }

        return deletedEdges;
    }

    private HashSet<Long> removeSingletonEdgesInForests(HashSet<Long> deletedNodes) {
        HashSet<Long> deletedEdges = new HashSet<>();
        while (hasSingletonPairs(degree_pairs)) {
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
//                            System.out.println("====remove single edge  " + r);

                            for (Map.Entry<Integer, SpanningForests> sp_forest_mapkey : this.dforests.dforests.entrySet()) {
                                int level = sp_forest_mapkey.getKey();
                                SpanningForests sp_forest = sp_forest_mapkey.getValue();
                                int sp_tree_idx;
                                if ((sp_tree_idx = sp_forest.findTreeIndex(r)) != -1) {
                                    SpanningTree sp_tree = sp_forest.trees.get(sp_tree_idx);
                                    sp_tree.rbtree.delete((Integer) r.getProperty("pFirstID" + level));
                                    sp_tree.rbtree.delete((Integer) r.getProperty("pSecondID" + level));
                                    sp_tree.deleteAdditionalInformationByRelationship(r);

                                    if (sp_tree.rbtree.root == nil) {
                                        sp_forest.trees.remove(sp_tree_idx);
                                    }
                                }
                            }

                            /**
                             * although single edges are deleted, the remove type is multiple.
                             * It is used to make sure the highway knows the information of connection between two highway nodes.
                             * See the OneNote "backbone  --> Index organization"
                             * **/
//                            updateLayerIndex(r, layer_index, nodesToHighWay, multiple);
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


    private boolean hasSingletonPairs(TreeMap<Pair<Integer, Integer>, ArrayList<Long>> c_degree_pairs) {
        for (Map.Entry<Pair<Integer, Integer>, ArrayList<Long>> e :
                c_degree_pairs.entrySet()) {
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
            tx.success();
        }


        this.degree_pairs_sum = 0;
        for (Map.Entry<Pair<Integer, Integer>, ArrayList<Long>> dps : this.degree_pairs.entrySet()) {
            degree_pairs_sum += dps.getValue().size();
        }

    }

    private Pair<Integer, Integer> updateThreshold(double percentage) {
        ArrayList<Pair<Integer, Integer>> t_degree_pair = new ArrayList<>(this.degree_pairs.keySet());
        if (t_degree_pair.isEmpty()) {
            return null;
        }
        System.out.println("Find updated threshold:");
        int max = (int) (this.numberOfEdges * percentage);

        System.out.println("percentage: " + max + "   number of nodes :" + cn + "   degree pair sum( number of edges ):" + degree_pairs_sum + "\nIs the Max is less than the number of degree pair ?" + (max < this.degree_pairs_sum));
//        for (Map.Entry<Pair<Integer, Integer>, ArrayList<Long>> p : this.degree_pairs.entrySet()) {
//            System.out.println(p.getKey() + "  ==>>> " + p.getValue());
//        }

        //get the last degree pair
        if (max > this.degree_pairs_sum) {
            return t_degree_pair.get(t_degree_pair.size() - 1);
        }


        int t_num = 0;
        int idx;


        for (idx = 0; idx < t_degree_pair.size(); idx++) {
            Pair<Integer, Integer> key = t_degree_pair.get(idx);
            t_num += this.degree_pairs.get(key).size();
            if (t_num >= max) {
                break;
            }
        }

//        System.out.println(t_num + "   " + idx);

        //idx is the index of the first degree pair which summation number is greater than |E|*p
        if (0 <= idx && idx < t_degree_pair.size()) {
            return t_degree_pair.get(idx);
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
        String src_db_name = graphsize + "_" + degree + "_" + dimension + "_Level" + src_level;
        String dest_db_name = graphsize + "_" + degree + "_" + dimension + "_Level" + dest_level;
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

    private void createIndexFolder() {
        String folder = "/home/gqxwolf/mydata/projectData/BackBone/indexes/backbone_" + graphsize + "_" + degree + "_" + dimension;
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
     */
    private void indexBuild() {
        System.out.println("============== index building process");
        long overallIndex = 0;
        int maxlevel = this.deletedEdges_layer.size();
        for (int l = 0; l < maxlevel; l++) {
            int level = l;


            int nextlevel = level + 1;
            HashSet<Long> remind_nodes = new HashSet<>();
            if (nextlevel != maxlevel) {
                remind_nodes = getNodeListAtLevel(nextlevel);
            }

            String sub_folder_str = "/home/gqxwolf/mydata/projectData/BackBone/indexes/backbone_" + graphsize + "_" + degree + "_" + dimension + "/level" + level;
            File sub_folder_f = new File(sub_folder_str);
            if (sub_folder_f.exists()) {
                sub_folder_f.delete();
            }
            sub_folder_f.mkdirs();

            HashSet<Long> de = deletedEdges_layer.get(level);

            String graph_db_folder = graphsize + "_" + degree + "_" + dimension + "_Level" + level;
            Neo4jDB neo4j_level = new Neo4jDB(graph_db_folder);
            System.out.println(neo4j_level.DB_PATH + "   deleted edges:" + de.size());
            System.out.println(level + " " + nextlevel + " " + maxlevel + "  size of remind nodes " + remind_nodes.size());
            neo4j_level.startDB(true);
            GraphDatabaseService graphdb_level = neo4j_level.graphDB;

            long numIndex = 0;
            long sizeOverallSkyline = 0;
            try (Transaction tx = graphdb_level.beginTx()) {
                ResourceIterable<Node> allnodes_iteratable = graphdb_level.getAllNodes();
                ResourceIterator<Node> allnodes_iter = allnodes_iteratable.iterator();

                BufferedWriter writer = null;

                while (allnodes_iter.hasNext()) {
                    HashMap<Long, myNode> tmpStoreNodes = new HashMap();
                    Node node = allnodes_iter.next();
                    long nodeID = node.getId();
//                    System.out.println("process node :" + nodeID);
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
                                ArrayList<path> new_paths = p.expand(neo4j_level, de);
                                for (path np : new_paths) {
//                                    System.out.println("    " + np);
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
                                if (remind_nodes.size() != 0 && remind_nodes.contains(p.endNode)) {
                                    sum++;
                                } else if (remind_nodes.size() == 0) {
                                    sum++;
                                }
                            }
                        }
                    }

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
                                    if (level != (maxlevel - 1)) {
                                        if (remind_nodes.contains(p.endNode)) {
                                            writer.write(nodeid + " " + p.costs[0] + " " + p.costs[1] + " " + p.costs[2] + "\n");
                                        }
                                    } else {
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

    private HashSet<Long> getNodeListAtLevel(int nextlevel) {
        HashSet<Long> nodeList = new HashSet<Long>();
        String graph_db_folder = graphsize + "_" + degree + "_" + dimension + "_Level" + nextlevel;
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
