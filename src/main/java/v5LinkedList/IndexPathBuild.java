package v5LinkedList;

import Neo4jTools.Neo4jDB;
import configurations.ProgramProperty;
import javafx.util.Pair;
import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.*;

import java.io.File;
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
    private double percentage = 0.1;
    public ProgramProperty prop = new ProgramProperty();
    public String city_name;


    //Pair <sid_degree,did_degree> -> list of the relationship id that the degrees of the start node and end node are the response given pair of key
    TreeMap<Pair<Integer, Integer>, ArrayList<Long>> degree_pairs = new TreeMap(new PairComparator());

    //deleted edges record the relationship deleted in each layer, the index of each layer is based on the expansion on previous level graph
    ArrayList<HashSet<Long>> deletedEdges_layer = new ArrayList<>();
    private int degree_pairs_sum;

    public static void main(String args[]) throws CloneNotSupportedException {
        long start = System.currentTimeMillis();
        IndexPathBuild index = new IndexPathBuild();
        index.city_name = "ny_USA";
        index.build();
        long end = System.currentTimeMillis();
        System.out.println("Total Running time " + (end - start) * 1.0 / 1000 + "s  ~~~~~ ");
    }

    private void build() throws CloneNotSupportedException {
        initLevel();
        construction();
    }

    private void initLevel() {
        int currentLevel = 0;
//        String sub_db_name = graphsize + "_" + degree + "_" + dimension + "_Level" + currentLevel;

//        String sub_db_name = city_name + "_Level0";
//        String sub_db_name = graphsize + "_" + samenode_t + "_Level" + currentLevel;
        String sub_db_name = "sub_ny_USA_level0";
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
        String src_db_name = graphsize + "_" + samenode_t + "_Level" + src_level;
        String dest_db_name = graphsize + "_" + samenode_t + "_Level" + dest_level;

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


    private long handleUpperLevelGraph(int currentLevel, Pair<Integer, Integer> threshold_p, int threshold_t) {
        Hashtable<Long, ArrayList<Long>> nodesToHighWay = new Hashtable<>();
//        String sub_db_name = city_name + "_Level" + currentLevel;
        String sub_db_name = graphsize + "_" + samenode_t + "_Level" + currentLevel;

        String prefix = "/home/gqxwolf/mydata/projectData/BackBone/";

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
            sptree_base.EulerTourStringWiki(0);
//            deletedEdges.addAll(removeSingletonEdgesAtLevelZero(sptree_base, deletedNodes));
            getDegreePairs();
            threshold_p = updateThreshold(percentage);
            this.dforests.createBase(sptree_base);
            System.out.println("Finish finding the level 0 spanning tree..........");
        } else {
            updateNeb4jConnectorInDynamicForests();
        }


        boolean deleted = removeLowerDegreePairEdgesByThreshold(threshold_p, threshold_t, deletedNodes, deletedEdges);
        System.out.println("Removing the edges in level " + currentLevel + "  with degree threshold  : " + threshold_p);

//        getDegreePairs();
//        deletedEdges.addAll(removeSingletonEdgesInForests(deletedNodes));
//        long numberOfNodes = neo4j.getNumberofNodes();
//        long post_n = neo4j.getNumberofNodes();
//        long post_e = neo4j.getNumberofEdges();
//
//        String textFilePath = prefix + "busline_" + this.graphsize + "_" + this.samenode_t + "/level" + currentLevel + "/";
//        neo4j.saveGraphToTextFormation(textFilePath);
//
//        System.out.println("pre:" + pre_n + " " + pre_e + "  post:" + post_n + " " + post_e + "   # of deleted Edges:" + deletedEdges.size());
//        this.deletedEdges_layer.add(deletedEdges);
        neo4j.closeDB();
//        return numberOfNodes;
        return 0;
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

            if (dforests.isTreeEdge(r.getId())) {
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
//                    updateDynamicForest(level_r, l_idx, r, replacement_edge);
//                    deleteRelationshipFromDB(r, deletedNodes);
                    tx.success();
                    return true;
                }
            } else {
//                deleteRelationshipFromDB(r, deletedNodes);
                tx.success();
                return true;
            }

            tx.success();
        }
        return false;
    }

    private void updateNeb4jConnectorInDynamicForests() {
        for (Map.Entry<Integer, SpanningForests> sp_forests_e : this.dforests.dforests.entrySet()) {
            for (SpanningTree sp_tree : sp_forests_e.getValue().trees) {
                sp_tree.neo4j = this.neo4j;
            }
        }
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