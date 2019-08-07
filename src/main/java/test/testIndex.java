package test;

import Index.Chain;
import Neo4jTools.BNode;
import Neo4jTools.Neo4jDB;
import javafx.util.Pair;
import org.neo4j.graphdb.*;

import java.util.*;

public class testIndex {
    int graph_size = 12;
    double samnode_t = 0;
    GraphDatabaseService graphdb;
    TreeMap<Pair<Integer, Integer>, ArrayList<Long>> degree_pairs = new TreeMap(new PairComparator());
    HashSet<Long> bridges = new HashSet<>();
    HashSet<Long> CutVerties = new HashSet<>();
    HashMap<Long, Integer> tt = new HashMap<>();
    ArrayList<Chain> chainsInGraph = new ArrayList<>(); //keep the chains of the graphs
    int numberofchains = 0;
    private Neo4jDB neo4j;

    public static void main(String args[]) {
        testIndex i = new testIndex();
        i.test(0);
    }

    private void test(int currentLevel) {
        String sub_db_name = graph_size + "-" + samnode_t + "-" + "Level" + currentLevel;
        neo4j = new Neo4jDB(sub_db_name);
        neo4j.startDB(false);
        graphdb = neo4j.graphDB;
        getDegreePairs();
        long cn = neo4j.getNumberofNodes();
        DFS();
        for(Chain c: this.chainsInGraph){
            System.out.println(c);
        }
        neo4j.closeDB();
    }


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

    public int DFS() {
        initialzeGraphAttrs();
        int cc_num_node = -1;
        try (Transaction tx = graphdb.beginTx()) {

            Stack<Node> stack = new Stack<>();
            Node startNode = neo4j.getRandomNode();
            if (startNode == null) {
                return cc_num_node;
            }
            startNode.setProperty("fromRelID", -1L);
            stack.push(startNode);
//
            int dfs_order = 0;

            while (!stack.isEmpty()) {
                Node n = stack.pop();
                boolean n_visited = (boolean) n.getProperty("visited");
//                System.out.println(n);

                if (!n_visited) {
                    n.setProperty("visited", true);
                    n.setProperty("dfs_order", dfs_order);
                    dfs_order++;
//                    System.out.println(n);

                    Iterable<Relationship> relIterable = n.getRelationships(Direction.BOTH);
                    Iterator<Relationship> relIter = relIterable.iterator();

                    while (relIter.hasNext()) {
                        Relationship rel = relIter.next();
                        Node en = neo4j.getoutgoingNode(rel, n);


                        stack.add(en);
                        boolean en_visited = (boolean) en.getProperty("visited");
                        if (!en_visited) {
                            en.setProperty("fromRelID", rel.getId());
                        }
                    }
                }
            }

            cc_num_node = dfs_order;
            //mark the edges in the dfs tree
            for (long i = 0; i < dfs_order; i++) {
                Node v = graphdb.findNode(BNode.BusNode, "dfs_order", i);
                long v_from_rel_id = (long) v.getProperty("fromRelID");
                if (v_from_rel_id != -1) {
                    graphdb.getRelationshipById(v_from_rel_id).setProperty("isInDFSTree", true);
                }
            }
            tx.success();

            int chain_num = 1;

            for (long i = 0; i < dfs_order; i++) {
                Node v = graphdb.findNode(BNode.BusNode, "dfs_order", i);
                Iterable<Relationship> relIterable = v.getRelationships(Direction.BOTH);
                Iterator<Relationship> relIter = relIterable.iterator();
                while (relIter.hasNext()) {
                    Relationship rel = relIter.next();
                    Node en = neo4j.getoutgoingNode(rel, v);

                    boolean isDFSEdge = (boolean) rel.getProperty("isInDFSTree");
                    int en_dfs_order = (int) en.getProperty("dfs_order");
                    if (!isDFSEdge && i < en_dfs_order) {
                        rel.setProperty("isInCycle", true);
                        //trace back from v to en
                        traceBackFromDFStree(v, en, chain_num);
                        numberofchains++;
                        chain_num++;
                    }
                }
            }


            {
                this.bridges.clear();
                ResourceIterable<Relationship> rel_Iterable = graphdb.getAllRelationships();
                ResourceIterator<Relationship> rel_Iter = rel_Iterable.iterator();
                int num_bridge = 0;
                while (rel_Iter.hasNext()) {
                    Relationship rel = rel_Iter.next();
                    if (!(boolean) rel.getProperty("isInCycle")) {
                        this.bridges.add(rel.getId());
                        num_bridge++;
                    }
                }


                this.CutVerties.clear();
                for (Chain c : this.chainsInGraph) {
                    if (c.isCycle && c.index != 1) {
                        this.CutVerties.add(c.nodeList.get(0));
                    }
                }

                tx.success();
            }
            tx.success();

        }

        return cc_num_node;
//        neo4j.closeDB();
    }

    private void traceBackFromDFStree(Node sv, Node ev, int chain_num) {
        try (Transaction tx = graphdb.beginTx()) {

            if (tt.containsKey(sv.getId())) {
                this.tt.put(sv.getId(), tt.get(sv.getId()) + 1);
            } else {
                this.tt.put(sv.getId(), 1);
            }
            Chain chain = new Chain();
            chain.index = chain_num;
            chain.add(sv.getId());
            boolean en_cycle_visited = (boolean) sv.getProperty("cycle_visited");
            if (!en_cycle_visited) {
                sv.setProperty("cycle_visited", true);
            }

            Node cur_node = ev;
            en_cycle_visited = (boolean) cur_node.getProperty("cycle_visited");

            while (!en_cycle_visited) {
                Relationship rel = graphdb.getRelationshipById((Long) cur_node.getProperty("fromRelID"));
                rel.setProperty("isInCycle", true);
                if (tt.containsKey(cur_node.getId())) {
                    this.tt.put(cur_node.getId(), tt.get(cur_node.getId()) + 1);
                } else {
                    this.tt.put(cur_node.getId(), 1);
                }
                chain.add(cur_node.getId());

                cur_node.setProperty("cycle_visited", true);
                cur_node = neo4j.getoutgoingNode(rel, cur_node);
                en_cycle_visited = (boolean) cur_node.getProperty("cycle_visited");
            }

            if (cur_node.getId() == sv.getId()) {
                chain.isCycle = true;
            }else{
                if (tt.containsKey(cur_node.getId())) {
                    this.tt.put(cur_node.getId(), tt.get(cur_node.getId()) + 1);
                } else {
                    this.tt.put(cur_node.getId(), 1);
                }
                chain.add(cur_node.getId());
            }

            this.chainsInGraph.add(chain);
            tx.success();
        }
    }


    public void initialzeGraphAttrs() {
        try (Transaction tx = graphdb.beginTx()) {
            ResourceIterable<Node> node_Iterable = graphdb.getAllNodes();
            ResourceIterator<Node> node_Iter = node_Iterable.iterator();

            while (node_Iter.hasNext()) {
                Node n = node_Iter.next();
                n.setProperty("visited", false);
                n.setProperty("dfs_order", -1);
                n.setProperty("cycle_visited", false);
            }
            tx.success();
        }

        try (Transaction tx = graphdb.beginTx()) {
            ResourceIterable<Relationship> rel_Iterable = graphdb.getAllRelationships();
            ResourceIterator<Relationship> rel_Iter = rel_Iterable.iterator();

            while (rel_Iter.hasNext()) {
                Relationship rel = rel_Iter.next();
                rel.setProperty("isInCycle", false);
                rel.setProperty("isInDFSTree", false);
            }
            tx.success();
        }

//        System.out.println("init: nodes-" + neo4j.getNumberofNodes() + " edges-" + neo4j.getNumberofEdges());
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