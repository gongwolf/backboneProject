package v3;

import Neo4jTools.Neo4jDB;
import configurations.ProgramProperty;
import javafx.util.Pair;
import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

public class indexWithDynamic {

    public ProgramProperty prop = new ProgramProperty();
    GraphDatabaseService graphdb;
    int graph_size = 14;
    double samnode_t = 0;
    //Pair <sid_degree,did_degree> -> list of the relationship id that the degrees of the start node and end node are the response given pair of key
    TreeMap<Pair<Integer, Integer>, ArrayList<Long>> degree_pairs = new TreeMap(new PairComparator());
    DynamicForests dforests;
    long cn;
    private Neo4jDB neo4j;

    public static void main(String args[]) {
        indexWithDynamic index = new indexWithDynamic();
        index.build();
    }

    private void build() {
        initLevel();
        construction();
    }

    private void construction() {
        int i = 0;
        int currentLevel = 0;
        Pair<Integer, Integer> threshold = new Pair<>(2, 2);
        int t_threshold = threshold.getKey() + threshold.getValue();
        do {
            i++;
            int upperlevel = currentLevel + 1;
            System.out.println("===============  level:" + upperlevel + " ==============");
            System.out.println("threshold:" + threshold.getKey() + "+" + threshold.getValue() + " = " + t_threshold);

            //copy db from previous level
            copyToHigherDB(currentLevel, upperlevel);
            //handle the degrees pairs
            cn = handleUpperLevelGraph(upperlevel, threshold, t_threshold);


        } while (false);


    }

    private void initLevel() {
        int currentLevel = 0;
        String sub_db_name = graph_size + "-" + samnode_t + "-" + "Level" + currentLevel;
        neo4j = new Neo4jDB(sub_db_name);
        System.out.println(neo4j.DB_PATH);
        neo4j.startDB();
        graphdb = neo4j.graphDB;
        getDegreePairs();
        long cn = neo4j.getNumberofNodes();
        neo4j.closeDB();
        //initialize the data structure that store multi-layer spanning forest.
        this.dforests = new DynamicForests();
    }


    private long handleUpperLevelGraph(int currentLevel, Pair<Integer, Integer> threshold_p, int threshold_t) {

        boolean deleted = true;


        String sub_db_name = graph_size + "-" + samnode_t + "-" + "Level" + currentLevel;
        neo4j = new Neo4jDB(sub_db_name);
        neo4j.startDB();
        graphdb = neo4j.graphDB;
        long pre_n = neo4j.getNumberofNodes();
        long pre_e = neo4j.getNumberofEdges();

        if (currentLevel == 1) {
            dforests = new DynamicForests();
            SpanningTree sptree_base = new SpanningTree(neo4j, true);
            System.out.println("=======================================");
            TreeMap<Long, GraphNode> graph_node_spanning_rb_map = new TreeMap<>();
            System.out.println(graph_node_spanning_rb_map.size());
            System.out.println(sptree_base.EulerTourString(graph_node_spanning_rb_map));
            System.out.println(graph_node_spanning_rb_map.size() + "\n------------------\nSingle pair degree in tree:");

            for (Map.Entry<Long, GraphNode> nentry : graph_node_spanning_rb_map.entrySet()) {
                if (nentry.getValue().frist == nentry.getValue().last) {
                    System.out.println(nentry.getKey());
                }
            }
            System.out.println("------------------");
//            sptree_base.rbtree.printTree(sptree_base.rbtree.root);
            removeSingletonEdges(graph_node_spanning_rb_map, sptree_base);
            System.out.println("------------------");
            System.out.println("root");
            sptree_base.rbtree.root.print();

            this.dforests.createBase(sptree_base);
        }


        while (deleted) {
            //remove edges less than threshold
            deleted = removeLowerDegreePairEdgesByThreshold(threshold_p, threshold_t);
//            getDegreePairs();
//            removeSingletonEdges(graph_node_spanning_rb_map, sptree_base);

            /**
             * Compare the number of components before the deletion and after the deletion
             */
//            int total_num_node = Math.toIntExact(neo4j.getNumberofNodes());
//            System.out.println((total_num_node == post_cc_num_node) + "     " + total_num_node + "  " + post_cc_num_node);
//            if (total_num_node != post_cc_num_node) {
//                System.out.println("terminated the program");
//                System.exit(0);
//            }

        }


        long numberOfNodes = neo4j.getNumberofNodes();
        long post_n = neo4j.getNumberofNodes();
        long post_e = neo4j.getNumberofEdges();
        System.out.println("pre:" + pre_n + " " + pre_e + "  post:" + post_n + " " + post_e);
        neo4j.closeDB();
        return numberOfNodes;
    }


    private void removeSingletonEdges(TreeMap<Long, GraphNode> graph_node_spanning_rb_map, SpanningTree sptree_base) {
        while (hasSingletonPairs(degree_pairs)) {
            long pre_edge_num = neo4j.getNumberofEdges();
            long pre_node_num = neo4j.getNumberofNodes();
            long pre_degree_num = degree_pairs.size();
            int sum_single = 0;
            try (Transaction tx = graphdb.beginTx()) {
                for (Map.Entry<Pair<Integer, Integer>, ArrayList<Long>> e : degree_pairs.entrySet()) {
                    if (e.getKey().getValue() == 1 || e.getKey().getKey() == 1) {
                        sum_single += e.getValue().size();
                        for (Long rel_id : e.getValue()) {
                            Relationship r = graphdb.getRelationshipById(rel_id);
                            Node sNode = r.getStartNode();
                            Node eNode = r.getEndNode();

//                            Todo:delete single edge from spanning tree
//                            System.out.println(r);
//                            System.out.println(r.getProperty("pFirstID") + "     " + r.getProperty("pSecondID"));
                            sptree_base.rbtree.delete((Integer) r.getProperty("pFirstID"));
                            sptree_base.rbtree.delete((Integer) r.getProperty("pSecondID"));
//
//                            System.out.println(sNode.getId() + " first " + graph_node_spanning_rb_map.get(sNode.getId()).frist.item);
//                            System.out.println(sNode.getId() + " last  " + graph_node_spanning_rb_map.get(sNode.getId()).last.item);
//                            System.out.println(eNode.getId() + " first " + graph_node_spanning_rb_map.get(eNode.getId()).frist.item);
//                            System.out.println(eNode.getId() + " last  " + graph_node_spanning_rb_map.get(eNode.getId()).last.item);
//                            System.out.println("====");

                            r.delete();

                            if (sNode.getDegree(Direction.BOTH) == 0) {
                                sNode.delete();
                            }
                            if (eNode.getDegree(Direction.BOTH) == 0) {
                                eNode.delete();
                            }
//                            return;
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
    }

    /**
     * Copy the graph from the src_level to the dest_level,
     * the dest level graph used to shrink and build index on upper level.
     *
     * @param src_level
     * @param dest_level
     */
    private void copyToHigherDB(int src_level, int dest_level) {
        String dest_db_name = graph_size + "-" + samnode_t + "-" + "Level" + dest_level;
        String src_db_name = graph_size + "-" + samnode_t + "-" + "Level" + src_level;
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


    private boolean removeLowerDegreePairEdgesByThreshold(Pair<Integer, Integer> threshold_p, int threshold_t) {
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
                        Node sNode = r.getStartNode();
                        Node eNode = r.getEndNode();


                        int level_r = (int) r.getProperty("level");

                        System.out.println(r + " is a tree edge ? " + dforests.isTreeEdge(r) + "  level:" + level_r);
                        int l_idx = level_r;
                        while (l_idx >= 0) {
                            if (!dforests.replacement(r, l_idx)) {
                                l_idx--;
                            } else {
                                break;
                            }
                        }

                        r.delete();


                        if (sNode.getDegree(Direction.BOTH) == 0) {
                            sNode.delete();
                        }
                        if (eNode.getDegree(Direction.BOTH) == 0) {
                            eNode.delete();
                        }
                        deleted = true;
                        break;
                    }
                }
                //test ternination
                return false;
            }
        }


        return deleted;
    }


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
}
