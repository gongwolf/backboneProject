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

    public static void main(String args[]) throws CloneNotSupportedException {
        indexWithDynamic index = new indexWithDynamic();
        index.build();
    }

    private void build() throws CloneNotSupportedException {
        initLevel();
        construction();
    }

    private void construction() throws CloneNotSupportedException {
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
            System.exit(0);
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
        cn = neo4j.getNumberofNodes();
        neo4j.closeDB();
        //initialize the data structure that store multi-layer spanning forest.
        this.dforests = new DynamicForests();
    }


    private long handleUpperLevelGraph(int currentLevel, Pair<Integer, Integer> threshold_p, int threshold_t) throws CloneNotSupportedException {

        boolean deleted = true;
        String sub_db_name = graph_size + "-" + samnode_t + "-" + "Level" + currentLevel;
        neo4j = new Neo4jDB(sub_db_name);
        neo4j.startDB();
        graphdb = neo4j.graphDB;
        long pre_n = neo4j.getNumberofNodes();
        long pre_e = neo4j.getNumberofEdges();
        System.out.println("deal with level " + currentLevel + " graph at " + neo4j.DB_PATH);


        if (currentLevel == 1) {
            dforests = new DynamicForests();
            SpanningTree sptree_base = new SpanningTree(neo4j, true);
            System.out.println("=======================================");
            TreeMap<Long, GraphNode> graph_node_spanning_rb_map = new TreeMap<>();
            System.out.println("size of the graph_node_spanning_rb_map before finding the euler tour : " + graph_node_spanning_rb_map.size());
            System.out.println(sptree_base.EulerTourString(graph_node_spanning_rb_map, 0));
            System.out.println("size of the graph_node_spanning_rb_map after finding the euler tour : " + graph_node_spanning_rb_map.size());
            System.out.println("------------------");
            //remove single edge from level 0 spanning forest, because there only one spanning tree in this spanning forest
            removeSingletonEdges(sptree_base, 0);
            System.out.println("------------------");
            sptree_base.rbtree.root.print();
            this.dforests.createBase(sptree_base);
            System.out.println("=======================================");
        }


        while (deleted) {
            //remove edges less than threshold
            deleted = removeLowerDegreePairEdgesByThreshold(threshold_p, threshold_t);
            System.out.println("Does delete edges with lower degree pairs ?  " + deleted);
            for (Map.Entry<Integer, SpanningForests> sf : dforests.dforests.entrySet()) {
                for (SpanningTree sp_tree : sf.getValue().trees) {
                    sp_tree.rbtree.root.print();
                    System.out.println("---------------------------------");
                }
                System.out.println("===================================================");
            }
            getDegreePairs();
            System.out.println("updated the degree pairs information ......................");
            removeSingletonEdgesInForests();


            try(Transaction tx = neo4j.graphDB.beginTx()){
                ResourceIterable<Relationship> a = neo4j.graphDB.getAllRelationships();
                ResourceIterator<Relationship> b = a.iterator();
                while(b.hasNext()){
                    Relationship r = b.next();
                    System.out.println(r+"   "+r.getProperty("level"));
                    for(Map.Entry<String, Object> pp :r.getAllProperties().entrySet()){
                        System.out.println("  "+pp.getKey()+" <---->  "+pp.getValue());

                    }
                }
                tx.success();
            }
//            System.out.println("removed the single information ......................");
//            dforests.dforests.get(0).trees.get(0).rbtree.root.print();
//            break;

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

    private void removeSingletonEdgesInForests() {
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
                            Node sNode = r.getStartNode();
                            Node eNode = r.getEndNode();
                            System.out.print("deleted single relationship  ");

                            for (Map.Entry<Integer, SpanningForests> sp_forest : this.dforests.dforests.entrySet()) {
                                int level = sp_forest.getKey();
                                System.out.println(r + "   " + r.getProperty("pFirstID" + level) + "   " + r.getProperty("pSecondID" + level));
                                SpanningTree sp_tree;
                                if ((sp_tree = sp_forest.getValue().findTree(r)) != null) {
                                    sp_tree.rbtree.delete((Integer) r.getProperty("pFirstID" + level));
                                    sp_tree.rbtree.delete((Integer) r.getProperty("pSecondID" + level));
                                }
                            }

                            r.delete();

                            if (sNode.getDegree(Direction.BOTH) == 0) {
                                sNode.delete();
                            }
                            if (eNode.getDegree(Direction.BOTH) == 0) {
                                eNode.delete();
                            }
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


    private void removeSingletonEdges(SpanningTree sptree_base, int level) {
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
                            Node sNode = r.getStartNode();
                            Node eNode = r.getEndNode();

                            /*****
                             * Delete single edge from spanning tree,
                             * The order of the key will be kept the increase order.
                             * Also, it will not affect the connectivity of the graph.
                             *****/
                            sptree_base.rbtree.delete((Integer) r.getProperty("pFirstID" + level));
                            sptree_base.rbtree.delete((Integer) r.getProperty("pSecondID" + level));
                            System.out.println(r + " was removed");
                            r.delete();

                            sptree_base.deleteAdditionalInformationByRelationship(r);

                            if (sNode.getDegree(Direction.BOTH) == 0) {
                                sNode.delete();
                            }
                            if (eNode.getDegree(Direction.BOTH) == 0) {
                                eNode.delete();
                            }
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

//                System.out.println(r);

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


    private boolean removeLowerDegreePairEdgesByThreshold(Pair<Integer, Integer> threshold_p, int threshold_t) throws CloneNotSupportedException {
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

                        boolean flag = deleteEdge(r, neo4j);

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

    private boolean deleteEdge(Relationship r, Neo4jDB neo4j) throws CloneNotSupportedException {
        GraphDatabaseService graphdb = neo4j.graphDB;
        try (Transaction tx = graphdb.beginTx()) {
            System.out.println(r);
            System.out.println(graphdb);

            int level_r = (int) r.getProperty("level");
            Relationship replacement_edge = null;

            System.out.println(r + " is a tree edge ? " + dforests.isTreeEdge(r) + "  level:" + level_r);
            int l_idx = level_r;
            while (l_idx >= 0) {
                System.out.println("Finding the replacement relationship in level " + l_idx + " spanning tree");
                replacement_edge = dforests.replacement(r, l_idx);
                if (null == replacement_edge) {
                    l_idx--;
                } else {
                    break;
                }
            }

            System.out.println("level of deleted edge r : " + level_r + " level of replacement edge : " + l_idx);

            if (l_idx != -1) {
                updateDynamicForest(level_r, l_idx, r, replacement_edge);
                r.delete();
                System.out.println("end of the deletion of the relationship " + r);
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
