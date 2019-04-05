package v3;

import Neo4jTools.Neo4jDB;
import configurations.ProgramProperty;
import javafx.util.Pair;
import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

import static DataStructure.STATIC.nil;

public class indexWithDynamic2 {

    final int single = 1;
    final int multiple = 2;
    public ProgramProperty prop = new ProgramProperty();
    //index data structure
    public ArrayList<Hashtable<Long, Hashtable<Long, ArrayList<double[]>>>> index = new ArrayList();  //level --> <node id --->{ highway id ==> <skyline paths > }  >
    //intra index data structure that store the distance information between nodes in each layer
    public ArrayList<Hashtable<Long, Hashtable<Long, ArrayList<double[]>>>> intra_index = new ArrayList();  //level --> <node id --->{ highway id ==> <skyline paths > }  >
    public ArrayList<Hashtable<Long, ArrayList<Long>>> nodesToHighway_index = new ArrayList();
    int graphsize = 30;
    int degree = 3;
    int dimension = 3;
    GraphDatabaseService graphdb;
    double percentage = 0.3;
    //Pair <sid_degree,did_degree> -> list of the relationship id that the degrees of the start node and end node are the response given pair of key
    TreeMap<Pair<Integer, Integer>, ArrayList<Long>> degree_pairs = new TreeMap(new PairComparator());
    DynamicForests dforests;
    long cn;
    int degree_pairs_sum;
    private Neo4jDB neo4j;
    private long numberOfNodes;

    public static void main(String args[]) throws CloneNotSupportedException {
        indexWithDynamic2 index = new indexWithDynamic2();
        index.build();
    }

    private void build() throws CloneNotSupportedException {
        initLevel();
        construction();
        createIndexFolder();
        printSummurizationInformation();
//        test();
    }


    private void test() {
        long source_node = 9;
        for (int l = 0; l <= 7; l++) {
            System.out.println("level " + l + " : " + this.nodesToHighway_index.get(l).get(source_node));
            ArrayList<Long> highways = this.nodesToHighway_index.get(l).get(source_node);
            if (highways != null) {
                for (long h_node : highways) {
                    readHighwaysInformation(h_node, l);
                }
            }
        }
    }

    private void readHighwaysInformation(long h_node, int level) {
        System.out.println("        highway node :" + h_node + "  " + this.index.get(level).get(h_node));
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

    private void printSummurizationInformation() {
        int i = 0;
        long overall = 0;
        for (Hashtable<Long, Hashtable<Long, ArrayList<double[]>>> layer_index : index) {
            writeToDisk(layer_index, i);
            long summation = 0;
            for (Map.Entry<Long, Hashtable<Long, ArrayList<double[]>>> layer_index_entry : layer_index.entrySet()) {
                for (Map.Entry<Long, ArrayList<double[]>> source_entryL : layer_index_entry.getValue().entrySet()) {
                    summation += source_entryL.getValue().size();
                }
            }
            System.out.println("there are " + summation + " indexes at level " + i++);
            overall += summation;
        }


        i = 0;
        long intra_overall = 0;
        for (Hashtable<Long, Hashtable<Long, ArrayList<double[]>>> intra_layer_index : intra_index) {
            writeIntraIndexToDisk(intra_layer_index, i);
            long summation = 0;
            for (Map.Entry<Long, Hashtable<Long, ArrayList<double[]>>> intra_layer_index_entry : intra_layer_index.entrySet()) {
                for (Map.Entry<Long, ArrayList<double[]>> source_entryL : intra_layer_index_entry.getValue().entrySet()) {
                    summation += source_entryL.getValue().size();
                }
            }
            System.out.println("there are " + summation + " intra-layer indexes at level " + i++);
            intra_overall += summation;
        }

        System.out.println("the total index size is " + overall + "=" + (overall / 2));
        System.out.println("the total intra index size is " + intra_overall + "=" + (intra_overall / 2));
        System.out.println("the total overall index size is " + (intra_overall + overall) + "/=" + ((intra_overall + overall) / 2));

        writeNodesToHighwayToDisk(nodesToHighway_index);
    }

    private void writeNodesToHighwayToDisk(ArrayList<Hashtable<Long, ArrayList<Long>>> nodes_to_highway_index) {
        BufferedWriter writer = null;
        try {
            String sub_folder_str = "/home/gqxwolf/mydata/projectData/BackBone/indexes/backbone_" + graphsize + "_" + degree + "_" + dimension + "/nodeToHighway_index";
            File sub_folder_f = new File(sub_folder_str);
            if (sub_folder_f.exists()) {
                sub_folder_f.delete();
            }
            sub_folder_f.mkdirs();


            int level = 0;
            for (Hashtable<Long, ArrayList<Long>> nodes_to_highway : nodes_to_highway_index) {
                String index_file_str = sub_folder_str + "/source_to_highway_index_level" + level + ".idx";
                writer = new BufferedWriter(new FileWriter(index_file_str));

                for (Map.Entry<Long, ArrayList<Long>> nodes_to_highway_entry : nodes_to_highway.entrySet()) {
                    StringBuilder sb = new StringBuilder();

                    long source_node_id = nodes_to_highway_entry.getKey();

                    sb.append(source_node_id).append(":");

                    ArrayList<Long> Highway_nodes = nodes_to_highway_entry.getValue();
                    for (long h_nodes : Highway_nodes) {
                        sb.append(h_nodes).append(" ");
                    }
                    writer.write(sb + "\n");
                }
                writer.close();
                level++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeToDisk(Hashtable<Long, Hashtable<Long, ArrayList<double[]>>> layer_index, int level) {
        BufferedWriter writer = null;
        try {
            String sub_folder_str = "/home/gqxwolf/mydata/projectData/BackBone/indexes/backbone_" + graphsize + "_" + degree + "_" + dimension + "/level" + level;
            File sub_folder_f = new File(sub_folder_str);
            if (sub_folder_f.exists()) {
                sub_folder_f.delete();
            }
            sub_folder_f.mkdirs();

            for (Map.Entry<Long, Hashtable<Long, ArrayList<double[]>>> layer_index_entry : layer_index.entrySet()) {
                long highway_node_id = layer_index_entry.getKey();
                String index_file_str = sub_folder_str + "/" + highway_node_id + ".idx";

                writer = new BufferedWriter(new FileWriter(index_file_str));

                for (Map.Entry<Long, ArrayList<double[]>> source_entry : layer_index_entry.getValue().entrySet()) {
                    long source_id = source_entry.getKey();
                    for (double[] costs : source_entry.getValue()) {
                        writer.write(source_id + " " + costs[0] + " " + costs[1] + " " + costs[2] + "\n");
                    }
                }

                writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeIntraIndexToDisk(Hashtable<Long, Hashtable<Long, ArrayList<double[]>>> layer_index, int level) {
        BufferedWriter writer = null;
        try {
            String sub_folder_str = "/home/gqxwolf/mydata/projectData/BackBone/indexes/backbone_" + graphsize + "_" + degree + "_" + dimension + "/intraLayer/level" + level;
            File sub_folder_f = new File(sub_folder_str);
            if (sub_folder_f.exists()) {
                sub_folder_f.delete();
            }
            sub_folder_f.mkdirs();

            for (Map.Entry<Long, Hashtable<Long, ArrayList<double[]>>> layer_index_entry : layer_index.entrySet()) {
                long highway_node_id = layer_index_entry.getKey();
                String index_file_str = sub_folder_str + "/" + highway_node_id + ".idx";

                writer = new BufferedWriter(new FileWriter(index_file_str));

                for (Map.Entry<Long, ArrayList<double[]>> source_entry : layer_index_entry.getValue().entrySet()) {
                    long source_id = source_entry.getKey();
                    for (double[] costs : source_entry.getValue()) {
                        writer.write(source_id + " " + costs[0] + " " + costs[1] + " " + costs[2] + "\n");
                    }
                }

                writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


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


    private Pair<Integer, Integer> updateThreshold(double percentage) {


        ArrayList<Pair<Integer, Integer>> t_degree_pair = new ArrayList<>(this.degree_pairs.keySet());

        if (t_degree_pair.isEmpty()) {
            return null;
        }

        System.out.println("========================== find updated threshold ==========================");

//        int max = (int) (this.degree_pairs_sum * percentage);
        int max = (int) (this.numberOfNodes * percentage);


        System.out.println(max + "  number of nodes :" + numberOfNodes + " degree pair sum:" + degree_pairs_sum + "  " + (max < this.degree_pairs_sum));


        if (max > this.degree_pairs_sum) {
            return t_degree_pair.get(t_degree_pair.size() - 1);
        }


        int t_num = 0;
        int idx = 0;


        while (t_num < max && idx < t_degree_pair.size()) {
            Pair<Integer, Integer> key = t_degree_pair.get(idx);
            t_num += this.degree_pairs.get(key).size();
            idx++;
        }

        //idx is the index of the first degree pair which summation number is greater than |E|*p
        if (0 <= idx && idx < t_degree_pair.size()) {
            return t_degree_pair.get(idx);
        }
        return null;
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
        numberOfNodes = neo4j.getNumberofEdges();
        neo4j.closeDB();
        //initialize the data structure that store multi-layer spanning forest.
        this.dforests = new DynamicForests();
    }

    private void printDegreePairs() {
        System.out.println("*********************** DEGREE PAIRS ***********************");
        for (Map.Entry<Pair<Integer, Integer>, ArrayList<Long>> dps : this.degree_pairs.entrySet()) {
            System.out.println(dps.getKey() + "    ----> number of pairs " + dps.getValue().size());
        }
        System.out.println("************************************************************");

    }


    private long handleUpperLevelGraph(int currentLevel, Pair<Integer, Integer> threshold_p, int threshold_t) throws CloneNotSupportedException {

        Hashtable<Long, Hashtable<Long, ArrayList<double[]>>> layer_index = new Hashtable<>();
        Hashtable<Long, Hashtable<Long, ArrayList<double[]>>> intra_layer_index = new Hashtable<>();
        Hashtable<Long, ArrayList<Long>> nodesToHighWay = new Hashtable<>();
        HashSet<Long> deletedNodes = new HashSet<>();

        boolean deleted = true;
        String sub_db_name = graphsize + "_" + degree + "_" + dimension + "_Level" + currentLevel;
        neo4j = new Neo4jDB(sub_db_name);
        neo4j.startDB(false);
        graphdb = neo4j.graphDB;
        long pre_n = neo4j.getNumberofNodes();
        long pre_e = neo4j.getNumberofEdges();
        System.out.println("deal with level " + currentLevel + " graph at " + neo4j.DB_PATH);


        if (currentLevel == 1) {
            dforests = new DynamicForests();
            SpanningTree sptree_base = new SpanningTree(neo4j, true);
            System.out.println("=======================================");
            sptree_base.EulerTourString(0);

            removeSingletonEdges(sptree_base, 0, layer_index, nodesToHighWay, deletedNodes);
            getDegreePairs();
            threshold_p = updateThreshold(percentage);
            this.dforests.createBase(sptree_base);
            System.out.println("================Finish finding the level 0 spanning tree =======================");
        } else {
            updateNeb4jConnectorInDynamicForests();
        }


        //remove edges less than threshold
        deleted = removeLowerDegreePairEdgesByThreshold(threshold_p, threshold_t, layer_index, nodesToHighWay, deletedNodes);
        System.out.println("Does delete edges with lower degree pairs ?  " + (deleted ? "Yes" : "No"));

        getDegreePairs();
        removeSingletonEdgesInForests(layer_index, nodesToHighWay, deletedNodes);
        System.out.println("update degree pairs information");


        long numberOfNodes = neo4j.getNumberofNodes();
        long post_n = neo4j.getNumberofNodes();
        long post_e = neo4j.getNumberofEdges();


        System.out.println("pre:" + pre_n + " " + pre_e + "  post:" + post_n + " " + post_e);

//        System.out.println("Before:Converging the layer index ");
//        printLayerIndex(layer_index);
//        System.out.println("Converging the layer index ");
        convergeLayerIndex(layer_index, nodesToHighWay);
//        System.out.println("After:Converging the layer index ");
//        printLayerIndex(layer_index);


        if (numberOfNodes != 0) {
            System.out.println("Clearing the layer index ");
            clearLayerIndex(layer_index, nodesToHighWay, deletedNodes, intra_layer_index);
//            printLayerIndex(layer_index);
            System.out.println("--------------");
//            printLayerIndex(intra_layer_index);
        }

        this.index.add(layer_index);
        this.intra_index.add(intra_layer_index);
        this.nodesToHighway_index.add(nodesToHighWay);


        System.out.println("==========================================");

        neo4j.closeDB();
        return numberOfNodes;
    }

    private void convergeLayerIndex(Hashtable<Long, Hashtable<Long, ArrayList<double[]>>> layer_index, Hashtable<Long, ArrayList<Long>> nodesToHighWay) {
        boolean converged = false;
        while (!converged) {
            boolean i_f = false;
            for (long highway_n_id : layer_index.keySet()) {
//                System.out.println(highway_n_id + "!!!!!");
//                boolean t_f = updatedHighwayofHnode(highway_n_id, layer_index, nodesToHighWay);
                boolean t_f = updatedHighwayofHnode1(highway_n_id, layer_index, nodesToHighWay);
                if (!i_f && t_f) { //If any of the highway_n_id updated the layer index, i_f is set to true.
                    i_f = true;
                }
            }
            //If there is no update operation in current iteration.
            if (!i_f) {
                converged = true;
            }
        }
    }


    /**
     * If the node is removed after the current index construction iteration, the corresponding information needs to be removed as well.
     * 1) remove the layer index information, the removed node can not be a highway
     * 2) remove the highway information of each source node sid, which is the nid is sid's highway node. after the deletion, nid can not be the highway node of sid any more.
     *
     * @param layer_index       the layer index
     * @param nodesToHighWay    the highway information of each node
     * @param deletedNodes      the nodes need to be removed
     * @param intra_layer_index the index of the intra-information
     * @return updated layeri_index, nodesToHighWay, intra_layer_index
     */
    private boolean clearLayerIndex(Hashtable<Long, Hashtable<Long, ArrayList<double[]>>> layer_index, Hashtable<Long, ArrayList<Long>> nodesToHighWay, HashSet<Long> deletedNodes, Hashtable<Long, Hashtable<Long, ArrayList<double[]>>> intra_layer_index) {
        try {
            for (long nid : deletedNodes) {
                //remove the node to it's highway if the highway id the nid
                if (layer_index.get(nid) != null) {
                    for (long sid : layer_index.get(nid).keySet()) {
                        if (nodesToHighWay.get(sid) != null) {
                            int idx_source_to_nid = nodesToHighWay.get(sid).indexOf(nid);
                            if (idx_source_to_nid != -1) {
                                nodesToHighWay.get(sid).remove(idx_source_to_nid);
                            }

                            if (nodesToHighWay.get(sid).isEmpty()) {
                                nodesToHighWay.remove(sid);
                            }
                        }
                    }
                }


                //if nid needs to be removed, put it information to the intra-index
                if (!intra_layer_index.containsKey(nid) && layer_index.get(nid) != null) {
                    System.out.println("remove:::::" + nid);
                    Hashtable<Long, ArrayList<double[]>> intra_source_to_deleted = new Hashtable<>();
                    for (Map.Entry<Long, ArrayList<double[]>> e : layer_index.get(nid).entrySet()) {
//                        if (deletedNodes.contains(e.getKey())) {
                        intra_source_to_deleted.put(e.getKey(), e.getValue());
                        if (nid == 11) {
                            System.out.println(e.getKey() + "    " + e.getValue());
                        }
                        intra_layer_index.put(nid, intra_source_to_deleted);
//                        }
                    }
                }

                layer_index.remove(nid); //remove the information of the node that is from itself to nid, nid is the highway of them.
//                nodesToHighWay.remove(nid); //remove the information of the node that is from nid to it's highways

            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private void printLayerIndex(Hashtable<Long, Hashtable<Long, ArrayList<double[]>>> layer_index) {
        for (Map.Entry<Long, Hashtable<Long, ArrayList<double[]>>> aa : layer_index.entrySet()) {
            System.out.println("Highway node  :" + aa.getKey());
            for (Map.Entry<Long, ArrayList<double[]>> bb : aa.getValue().entrySet()) {
                System.out.println("        source node :" + bb.getKey());
                for (double[] cc : bb.getValue()) {
                    System.out.println("                costs: [" + cc[0] + "," + cc[1] + "," + cc[2] + "]");
                }

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

    private void removeSingletonEdgesInForests(Hashtable<Long, Hashtable<Long, ArrayList<double[]>>> layer_index, Hashtable<Long, ArrayList<Long>> nodesToHighWay, HashSet<Long> deletedNodes) {
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
                            System.out.println("====remove single edge  " + r);

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

                            updateLayerIndex(r, layer_index, nodesToHighWay, multiple);
                            deleteRelationshipFromDB(r, deletedNodes);
                        }
                    }
                }
                tx.success();
            } catch (NotFoundException e) {
                System.out.println("no property found exception ");
                try (Transaction tx = neo4j.graphDB.beginTx()) {
                    ResourceIterable<Relationship> a = neo4j.graphDB.getAllRelationships();
                    ResourceIterator<Relationship> b = a.iterator();
                    while (b.hasNext()) {
                        Relationship r = b.next();
                        if (r.getId() == 87) {
                            System.out.println(r + "   " + r.getProperty("level"));
                            for (Map.Entry<String, Object> pp : r.getAllProperties().entrySet()) {
                                System.out.println("  " + pp.getKey() + " <---->  " + pp.getValue());

                            }
                        }
                    }
                    tx.success();
                }
                System.exit(0);

            }
            getDegreePairs();
            System.out.println("delete single : pre:" + pre_node_num + " " + pre_edge_num + " " + pre_degree_num + " " +
                    "single_edges:" + sum_single + " " +
                    "post:" + neo4j.getNumberofNodes() + " " + neo4j.getNumberofEdges() + " " +
                    "dgr_paris:" + degree_pairs.size());
        }
    }


    private void removeSingletonEdges(SpanningTree sptree_base, int level, Hashtable<Long, Hashtable<Long, ArrayList<double[]>>> layer_index, Hashtable<Long, ArrayList<Long>> nodesToHighWay, HashSet<Long> deletedNodes) {
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
                             * Delete single edge from level 0 spanning tree,
                             * The order of the key will be kept the increase order.
                             * Also, it will not affect the connectivity of the graph.
                             *****/
                            sptree_base.rbtree.delete((Integer) r.getProperty("pFirstID" + level));
                            sptree_base.rbtree.delete((Integer) r.getProperty("pSecondID" + level));

                            System.out.println(" ====remove single edge at level 0  " + r);

                            sptree_base.deleteAdditionalInformationByRelationship(r);

                            updateLayerIndex(r, layer_index, nodesToHighWay, single);
                            deleteRelationshipFromDB(r, deletedNodes);
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
            System.out.println("delete single : pre:" + pre_node_num + " " + pre_edge_num + " " + pre_degree_num + " " +
                    "single_edges:" + sum_single + " " +
                    "post:" + neo4j.getNumberofNodes() + " " + neo4j.getNumberofEdges() + " " +
                    "dgr_paris:" + degree_pairs.size());
        }
    }

    private void updateLayerIndex(Relationship r, Hashtable<Long, Hashtable<Long, ArrayList<double[]>>> layer_index, Hashtable<Long, ArrayList<Long>> nodesToHighWay, int type) {
        long s_degree = r.getStartNode().getDegree(Direction.BOTH);
        long e_degree = r.getEndNode().getDegree(Direction.BOTH);

        double[] costs = new double[3];

        costs[0] = (double) r.getProperty("EDistence");
        costs[1] = (double) r.getProperty("MetersDistance");
        costs[2] = (double) r.getProperty("RunningTime");


        //Remove single edge event
        if (type == single) {
            if (s_degree == 1) {
                Node highway_node = r.getEndNode();
                Node source_node = r.getStartNode();
                long h_id = highway_node.getId();
                long s_id = source_node.getId();
                addToLayerIndex(layer_index, h_id, s_id, costs, nodesToHighWay, true);
            } else if (e_degree == 1) {
                Node highway_node = r.getStartNode();
                Node source_node = r.getEndNode();
                long h_id = highway_node.getId();
                long s_id = source_node.getId();
                addToLayerIndex(layer_index, h_id, s_id, costs, nodesToHighWay, true);
            }
        } else if (type == multiple) {
            Node highway_node = r.getStartNode();
            Node source_node = r.getEndNode();
            long h_id = highway_node.getId();
            long s_id = source_node.getId();
            addToLayerIndex(layer_index, h_id, s_id, costs, nodesToHighWay, true);
//            System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");

            h_id = source_node.getId();
            s_id = highway_node.getId();
            addToLayerIndex(layer_index, h_id, s_id, costs, nodesToHighWay, true);
        }

//        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");


        //add the information that s_id's highway node is h_id

    }

    /**
     * Add the index from source node sid to the highway node h_id in layer index, and the meantime, it update the nodesToHighWay.
     *
     * @param layer_index    : the structure store the index of the current layer
     * @param h_id           : highway node id
     * @param s_id           : Source node id that can go to the highway node to higher layer
     * @param costs          : The costs from source node to highway node
     * @param nodesToHighWay : The data structure that store the information of source-highway mapping
     * @return Ture, if the h_id's index is updated. Otherwise return False.
     */
    public boolean addToLayerIndex(Hashtable<Long, Hashtable<Long, ArrayList<double[]>>> layer_index, long h_id, long s_id, double[] costs, Hashtable<Long, ArrayList<Long>> nodesToHighWay, boolean callRecursively) {
        //update the highway nodes information of s_id, s_id's highway is h_id
        if (!nodesToHighWay.containsKey(s_id)) {
            ArrayList<Long> highways = new ArrayList<>();
            highways.add(h_id);
            nodesToHighWay.put(s_id, highways);
        } else {
            ArrayList<Long> highways = nodesToHighWay.get(s_id);
            if (!highways.contains(h_id)) {
                highways.add(h_id);
                nodesToHighWay.put(s_id, highways);
            }
        }

        //if the layer index is updated, fix the index
        boolean updated = true;
        Hashtable<Long, ArrayList<double[]>> highway_index;
        if (!layer_index.containsKey(h_id)) {
            ArrayList<double[]> skyline_index = new ArrayList<>();
            skyline_index.add(costs);
            highway_index = new Hashtable<>();
            highway_index.put(s_id, skyline_index);
        } else {
            highway_index = layer_index.get(h_id);
            if (!highway_index.containsKey(s_id)) {
                ArrayList<double[]> skyline_index = new ArrayList<>();
                skyline_index.add(costs);
                highway_index.put(s_id, skyline_index);
            } else {
                ArrayList<double[]> skyline_index = highway_index.get(s_id);
                updated = addToSkyline(skyline_index, costs);
                highway_index.put(s_id, skyline_index);
            }
        }


        //if the new highway-source index is inserted or updated, updated the overall layer index based on the updated h_id index
        if (updated) {
            layer_index.put(h_id, highway_index);
        }

        boolean copied_flag = false;
        if (callRecursively) {
            copied_flag = copySourceNodeHighWayInformation(layer_index, h_id, s_id, costs, nodesToHighWay);
        }

//        System.out.println("end of the call addToLayerIndex");

        boolean result;
        if (!callRecursively) {
            result = updated;
        } else {
            result = updated || copied_flag;
        }

        return result;

    }

    /**
     * Foreach node h_id who is the highway node of sid.
     * Foreach node node_id whose highway is sid.
     * Update the highway index that from node_id to h_id,
     *
     * @param sid
     * @param layer_index
     * @param nodesToHighWay
     * @return If the index is updated return true, other wise return false.
     */

    private boolean updatedHighwayofHnode(long sid, Hashtable<Long, Hashtable<Long, ArrayList<double[]>>> layer_index, Hashtable<Long, ArrayList<Long>> nodesToHighWay) {
        boolean updated = false;
        try {
            if (nodesToHighWay.containsKey(sid)) {
                System.out.println("update the highway nodes that is the highway node of the " + sid + " current index " + nodesToHighWay.get(sid).toString());
                for (long h_id : nodesToHighWay.get(sid)) { //h_id is the node id who is the highway of sid
                    for (Map.Entry<Long, ArrayList<double[]>> sidAsHighway : layer_index.get(sid).entrySet()) {
                        long node_id = sidAsHighway.getKey();// the node id whose highway node is sid
                        if (h_id != node_id) {
                            ArrayList<double[]> sid_to_hid_costs = layer_index.get(h_id).get(sid); //get all skyline from sid to h_id
                            for (double[] s_t_h_cost : sid_to_hid_costs) {//costs from sid to h_id
                                for (double[] cost : sidAsHighway.getValue()) {//costs from nid to sid
                                    double[] newcosts = addCosts(s_t_h_cost, cost);
                                    System.out.println("    " + node_id + "-->" + sid + "-->" + h_id + " " + newcosts[0] + " " + newcosts[1] + " " + newcosts[2]);
                                    boolean t_updated = addToLayerIndex(layer_index, h_id, node_id, newcosts, nodesToHighWay, false);
                                    if (!updated && t_updated) {
                                        updated = true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (NullPointerException e) {
            System.out.println("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
            printLayerIndex(layer_index);
            e.printStackTrace();
            System.exit(0);
        }
        System.out.println("Updated " + updated);
        return updated;
    }


    private boolean updatedHighwayofHnode1(long h_id, Hashtable<Long, Hashtable<Long, ArrayList<double[]>>> layer_index, Hashtable<Long, ArrayList<Long>> nodesToHighWay) {
        boolean updated = false;
        try {
            if (layer_index.containsKey(h_id)) {
                Hashtable<Long, ArrayList<double[]>> sid_as_highway_info = new Hashtable<>(layer_index.get(h_id)); //get the node whose highway node is h_id
                for (Long source_node : sid_as_highway_info.keySet()) {
                    ArrayList<double[]> source_costs_list = layer_index.get(h_id).get(source_node);

                    if (layer_index.containsKey(source_node)) {
                        Hashtable<Long, ArrayList<double[]>> sub_node_as_highway_info = layer_index.get(source_node); //get the node whose highway node is sub_source_node

                        for (Long sub_source : sub_node_as_highway_info.keySet()) {

                            ArrayList<double[]> sub_costs_list = layer_index.get(source_node).get(sub_source);

                            if (h_id != sub_source) {
                                for (double[] costs_sub_to_source : sub_costs_list) {
                                    for (double[] costs_source_to_highway : source_costs_list) {
                                        double[] newcosts = addCosts(costs_source_to_highway, costs_sub_to_source);
//                                        System.out.println("    " + sub_source + "-->" + source_node + "-->" + h_id + " " + newcosts[0] + " " + newcosts[1] + " " + newcosts[2]);
                                        updated = addToLayerIndex(layer_index, h_id, sub_source, newcosts, nodesToHighWay, false);
                                        if (!updated && updated) {
                                            updated = true;
                                        }

                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (NullPointerException e) {
            System.out.println("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
            printLayerIndex(layer_index);
            e.printStackTrace();
            System.exit(0);
        }
//        System.out.println("Updated " + updated);
        return updated;
    }


    /**
     * Copy the sources node's information whose highway node is sid to the h_id's structure.
     *
     * @param layer_index    : the structure store the index of the current layer
     * @param h_id           : highway node id
     * @param s_id           : Source node id that can go to the highway node to higher layer
     * @param costs          : The costs from source node to highway node
     * @param nodesToHighWay : The data structure that store the information of source-highway mapping
     * @return Ture, if the h_id's index is updated after copy the source information of sid to it. Otherwise return False.
     */
    private boolean copySourceNodeHighWayInformation(Hashtable<Long, Hashtable<Long, ArrayList<double[]>>> layer_index, long h_id, long s_id, double[] costs, Hashtable<Long, ArrayList<Long>> nodesToHighWay) {
        boolean result = false;
        if (layer_index.containsKey(s_id)) {
            Hashtable<Long, ArrayList<double[]>> sid_as_highway_info = layer_index.get(s_id); //get the node whose highway node is sid
            for (Map.Entry<Long, ArrayList<double[]>> highway_information_entry : sid_as_highway_info.entrySet()) {
                long sub_source_node = highway_information_entry.getKey();
                if (h_id != sub_source_node) {
                    ArrayList<double[]> sub_source_skylines_costs = highway_information_entry.getValue();
                    for (double[] org_costs : sub_source_skylines_costs) {
                        double[] new_costs = addCosts(costs, org_costs); //new costs from sub_source_node -> sid -> hid
                        //sub_source node can go to the highway node h_id by using the new_costs
                        boolean updated = addToLayerIndex(layer_index, h_id, sub_source_node, new_costs, nodesToHighWay, false);

                        if (!result && updated) {
                            result = true;
                        }
                    }
                }
            }
        }
        return result;
    }

    private double[] addCosts(double[] costs, double[] org_costs) {
        int length = costs.length;
        double new_costs[] = new double[length];
        for (int i = 0; i < length; i++) {
            new_costs[i] = costs[i] + org_costs[i];
        }
        return new_costs;
    }


    public boolean addToSkyline(ArrayList<double[]> skyline_index, double[] costs) {
        int i = 0;
//        if (r.end.getPlaceId() == checkedDataId) {
//            System.out.println(r);
//        }
        if (skyline_index.isEmpty()) {
            skyline_index.add(costs);
        } else {
            boolean can_insert_np = true;
            for (; i < skyline_index.size(); ) {
                if (checkDominated(skyline_index.get(i), costs)) {
                    can_insert_np = false;
                    break;
                } else {
                    if (checkDominated(costs, skyline_index.get(i))) {
                        skyline_index.remove(i);
                    } else {
                        i++;
                    }
                }
            }
            if (can_insert_np) {
                skyline_index.add(costs);
                return true;
            }
        }
        return false;
    }

    private boolean checkDominated(double[] costs, double[] estimatedCosts) {
        for (int i = 0; i < costs.length; i++) {


            BigDecimal ci = new BigDecimal(String.valueOf(costs[i])).setScale(3, BigDecimal.ROUND_HALF_UP);
            BigDecimal ei = new BigDecimal(String.valueOf(estimatedCosts[i])).setScale(3, BigDecimal.ROUND_HALF_UP);

            if (ci.doubleValue() > ei.doubleValue()) {
                return false;
            }
        }
        return true;
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

    /**
     * Copy the graph from the src_level to the dest_level,
     * the dest level graph used to shrink and build index on upper level.
     *
     * @param src_level
     * @param dest_level
     */
    private void copyToHigherDB(int src_level, int dest_level) {
        String dest_db_name = graphsize + "_" + degree + "_" + dimension + "_Level" + dest_level;
        String src_db_name = graphsize + "_" + degree + "_" + dimension + "_Level" + src_level;
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


    private boolean removeLowerDegreePairEdgesByThreshold(Pair<Integer, Integer> threshold_p, int threshold_t, Hashtable<Long, Hashtable<Long, ArrayList<double[]>>> layer_index, Hashtable<Long, ArrayList<Long>> nodesToHighWay, HashSet<Long> deletedNodes) throws CloneNotSupportedException {
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

                        boolean flag = deleteEdge(r, neo4j, layer_index, nodesToHighWay, deletedNodes);

                        if (!deleted && flag) {
                            deleted = true;
                        }
                    }
                }
            }
            tx.success();
        }

        return deleted;
    }

    private boolean deleteEdge(Relationship r, Neo4jDB neo4j, Hashtable<Long, Hashtable<Long, ArrayList<double[]>>> layer_index, Hashtable<Long, ArrayList<Long>> nodesToHighWay, HashSet<Long> deletedNodes) {
        GraphDatabaseService graphdb = neo4j.graphDB;

        try (Transaction tx = graphdb.beginTx()) {

            int level_r = (int) r.getProperty("level");
            Relationship replacement_edge = null;

//            System.out.println("deleted the relationship " + r + " is a tree edge ? " + dforests.isTreeEdge(r) + "  level:" + level_r);
            if (dforests.isTreeEdge(r)) {
                int l_idx = level_r;
                while (l_idx >= 0) {
//                    System.out.println("Finding the replacement relationship in level " + l_idx + " spanning tree");
                    replacement_edge = dforests.replacement(r, l_idx);
                    if (null == replacement_edge) {
                        l_idx--;
                    } else {
                        break;
                    }
                }

//                System.out.println("level of deleted edge r : " + level_r + " level of replacement edge : " + l_idx);

                if (l_idx != -1 || r.getStartNode().getDegree(Direction.BOTH) == 1 || r.getEndNode().getDegree(Direction.BOTH) == 1) {
                    System.out.println("====remove tree edge  " + r);
                    updateDynamicForest(level_r, l_idx, r, replacement_edge);
                    updateLayerIndex(r, layer_index, nodesToHighWay, multiple);
                    deleteRelationshipFromDB(r, deletedNodes);
                    tx.success();
                    return true;

                }
            } else {
                System.out.println("====remove non-tree edge  " + r);
                updateLayerIndex(r, layer_index, nodesToHighWay, multiple);
                deleteRelationshipFromDB(r, deletedNodes);
                tx.success();
                return true;
            }

            tx.success();
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


    private void updateDynamicForest(int level_r, int l_idx, Relationship delete_edge, Relationship replacement_edge) {
        for (int i = level_r; i >= 0; i--) {
            if (i > l_idx) {
                dforests.dforests.get(i).deleteEdge(delete_edge);
            } else if (i <= l_idx) {
                dforests.dforests.get(i).replaceEdge(delete_edge, replacement_edge);
            }
        }
    }
}

