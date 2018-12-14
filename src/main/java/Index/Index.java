package Index;

import Neo4jTools.BNode;
import Neo4jTools.Neo4jDB;
import configurations.ProgramProperty;
import javafx.util.Pair;
import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.*;

import java.io.File;
import java.io.IOException;
import java.util.*;


public class Index {

    public ProgramProperty prop = new ProgramProperty();
    GraphDatabaseService graphdb;
    int graph_size = 1000;
    double samnode_t = 7;
    //Pair <sid_degree,did_degree> -> list of the relationship id that the degrees of the start node and end node are the response given pair of key
    TreeMap<Pair<Integer, Integer>, ArrayList<Long>> degree_pairs = new TreeMap(new PairComparator());
    HashSet<Long> bridges = new HashSet<>();
    HashSet<Long> CutVerties = new HashSet<>();
    HashSet<Long> bcEdges = new HashSet<>();
    HashMap<Long, Integer> tt = new HashMap<>();

    ArrayList<Chain> chainsInGraph = new ArrayList<>(); //keep the chains of the graphs

    int numberofchains = 0;
    private Neo4jDB neo4j;

    public static void main(String args[]) {
        Index i = new Index();
//        i.indexBuild(0);
        i.indexBuildV1(0);
    }

    private void indexBuildV1(int currentLevel) {
        int i = 0;
        Pair<Integer, Integer> threshold = new Pair<>(2, 2);
        int t_threshold = threshold.getKey() + threshold.getValue();

        String sub_db_name = graph_size + "-" + samnode_t + "-" + "Level" + currentLevel;
        neo4j = new Neo4jDB(sub_db_name);
        neo4j.startDB();
        graphdb = neo4j.graphDB;
        getDegreePairs();
        long cn = neo4j.getNumberofNodes();
        neo4j.closeDB();


        do {
            i++;
            int upperlevel = currentLevel + 1;
            System.out.println("===============  level:" + upperlevel + " ==============");
            System.out.println("threshold:" + threshold.getKey() + "+" + threshold.getValue() + " = " + t_threshold);

            //copy db from previous level
            copyToHigherDB(currentLevel, upperlevel);
            //handle the degrees pairs
            cn = handleUpperLevelGraph(upperlevel, threshold, t_threshold);

            threshold = updateThreshold(threshold, t_threshold);
            if (threshold != null) {
                t_threshold = threshold.getKey() + threshold.getValue();
                currentLevel = upperlevel;
            }
        } while (cn != 0 && threshold != null);
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
            removeSingletonEdges();
        }

        while (deleted) {
//            System.out.println("    -------------------         ");

            //find bridges on this graph
            FindTheBridges();
            //remove edges less than threshold
            deleted = removeLowerDegreePairEdgesByThreshold(threshold_p, threshold_t);
            getDegreePairs();
            removeSingletonEdges();
            int post_cc_num_node = FindTheBridges();
            int total_num_node = Math.toIntExact(neo4j.getNumberofNodes());
            System.out.println((total_num_node == post_cc_num_node) + "     " + total_num_node + "  " + post_cc_num_node);
            if (total_num_node != post_cc_num_node) {
                System.out.println("terminated the program");
                System.exit(0);
            }

        }
        long numberOfNodes = neo4j.getNumberofNodes();
        long post_n = neo4j.getNumberofNodes();
        long post_e = neo4j.getNumberofEdges();
        System.out.println("pre:" + pre_n + " " + pre_e + "  post:" + post_n + " " + post_e);
        neo4j.closeDB();
        return numberOfNodes;
    }

    private Pair<Integer, Integer> updateThreshold(Pair<Integer, Integer> threshold_p, int threshold_t) {
        ArrayList<Pair<Integer, Integer>> t_degree_pair = new ArrayList<>(this.degree_pairs.keySet());
        int idx = t_degree_pair.indexOf(threshold_p);
//        displayDegreePair(15);
//        System.out.println(threshold_p + "   " + t_degree_pair.size() + " " + idx);

        if (!t_degree_pair.isEmpty()) {
            if (idx == -1) {
                for (Pair<Integer, Integer> e : t_degree_pair) {
                    int t_t = e.getKey() + e.getValue();
                    if (t_t < threshold_t) {
                        continue;
                    } else if (t_t == threshold_t && e.getKey() > threshold_p.getKey()) {
                        return e;
                    } else if (t_t > threshold_t) {
                        return e;
                    }
                }
            } else {
                if (idx + 1 < t_degree_pair.size()) {
                    return t_degree_pair.get(idx + 1);
                } else {
                    return null;
                }
            }
        }
        return null;
    }

    private boolean removeLowerDegreePairEdgesByThreshold(Pair<Integer, Integer> threshold_p, int threshold_t) {
        int min_t = threshold_p.getKey() < threshold_p.getValue() ? threshold_p.getKey() : threshold_p.getValue();
        int i = 0;
        boolean deleted = false;
        try (Transaction tx = graphdb.beginTx()) {
            for (Map.Entry<Pair<Integer, Integer>, ArrayList<Long>> dp : this.degree_pairs.entrySet()) {
//                int min_tt = dp.getKey().getKey() < dp.getKey().getValue() ? dp.getKey().getKey() : dp.getKey().getValue();
                int min_tt = 2;
                int ct = dp.getKey().getKey() + dp.getKey().getValue();
                int sd = dp.getKey().getKey();
                int ed = dp.getKey().getValue();

                if (sd == 3 && ed == 3 && dp.getValue().size() == 10 && neo4j.getNumberofNodes() == 424) {
                    updateCurrentBridgeComponentBFS(bridges, dp.getValue());
                    ChainList chainl = FindCandidateChains(dp.getValue());

                    System.out.println("===============Chain Component =============");
//                    ChainComponents ccps = chainl.formChainComponent(neo4j);
//                    System.out.println(ccps);
//                    System.out.println("        -------------------------           ");
//                    for (ChainComponet cp : ccps.c_components) {
//                        for (long rid : cp.edges) {
//                            Relationship rr = this.graphdb.getRelationshipById(rid);
//                            System.out.println(rr + ":" + findNumberOfchainWithNode(rr.getStartNodeId()) + "    " + findNumberOfchainWithNode(rr.getEndNodeId()));
//                        }
//                    }
                    for (long rid : dp.getValue()) {
                        Relationship rr = this.graphdb.getRelationshipById(rid);
                        System.out.println(rr);
                        findNumberOfchainWithNode(rr.getStartNodeId());
                        findNumberOfchainWithNode(rr.getEndNodeId());
                        System.out.println("------------------------------");
                    }
                    System.out.println("===============Bridge Component =============");

                    for (Long rr : this.bcEdges) {
                        System.out.println(graphdb.getRelationshipById(rr));

                    }

                    System.out.println("===============Bridges ======================");


                    for (Long rr : this.bridges) {
                        System.out.println(graphdb.getRelationshipById(rr));
                    }

                    System.out.println("===============Bridges Nodes =================");

//                    for (Long rr : bridges_nodes) {
//                        System.out.println(graphdb.getNodeById(rr));
//                    }
//
                    System.out.println("===============ChainList=====================");

                    for (Chain c : chainl.chainlist) {
                        System.out.println(c);
                    }

                    System.out.println("===============Cut vertex =====================");

                    for (long c : this.CutVerties) {
                        System.out.println(c);
                    }

                    System.out.println("================================");
                    for (long rel : dp.getValue()) {
                        //&& !this.bcEdges.contains(rel)
                        if (!this.bridges.contains(rel)) {
                            Relationship r = graphdb.getRelationshipById(rel);
//                            findRelsInSameChain(r);
                            Node sNode = r.getStartNode();
                            Node eNode = r.getEndNode();
//                            if (!bridges_nodes.contains(sNode.getId()) && !bridges_nodes.contains(eNode.getId())) {
//                                System.out.print("remove lower degree pair:<" + dp.getKey().getKey() + "," + dp.getKey().getValue() + "> --- " + r);
//                                System.out.println("\n" + !bridges_nodes.contains(sNode.getId()) + " " + !bridges_nodes.contains(eNode.getId()));
//                            } else {
//                                System.out.println("kept the lower degree pair:<" + dp.getKey().getKey() + "," + dp.getKey().getValue() + "> --- " + r);
//                            }
                            System.out.println("remove lower degree pair:<" + dp.getKey().getKey() + "," + dp.getKey().getValue() + "> --- " + r);
                            System.out.println(sNode.getId() + ":" + findNumberOfchainWithNode(sNode.getId()) + "    " + eNode.getId() + ":" + findNumberOfchainWithNode(eNode.getId()));
                            System.out.println(FindNumberOfChains(r));
//                            r.delete();
//                            if (sNode.getDegree(Direction.BOTH) == 0) {
//                                sNode.delete();
//                            }
//                            if (eNode.getDegree(Direction.BOTH) == 0) {
//                                eNode.delete();
//                            }
                            System.out.println("--------------------------");
                        }
                    }


                    System.out.println("================================");
                    System.out.print(findNumberOfchainWithNode(404));


                    getDegreePairs();

                    removeSingletonEdges();
                    DFS();
                    System.out.println("terminated the program");
                    System.exit(0);
                }


                if ((ct < threshold_t) ||
                        (threshold_t == ct && sd < threshold_p.getKey()) ||
                        (sd == threshold_p.getKey() && ed == threshold_p.getValue()) ||
                        (ed == threshold_p.getKey() && sd == threshold_p.getValue())) {

                    updateCurrentBridgeComponentBFS(bridges, dp.getValue());
                    HashSet<Long> bridges_nodes = saftyDeleteRels(dp.getValue(), min_tt);
//                    for (long id : bridges_nodes) {
//                        System.out.println(id);
//                    }
                    for (long rel : dp.getValue()) {
                        //&& !this.bcEdges.contains(rel)
                        if (!this.bridges.contains(rel)) {

                            Relationship r = graphdb.getRelationshipById(rel);
                            if (!this.CutVerties.contains(r.getStartNodeId()) && !this.CutVerties.contains(r.getEndNodeId())) {
                                Node sNode = r.getStartNode();
                                Node eNode = r.getEndNode();
                                r.delete();
                                if (sNode.getDegree(Direction.BOTH) == 0) {
                                    sNode.delete();
                                }
                                if (eNode.getDegree(Direction.BOTH) == 0) {
                                    eNode.delete();
                                }
                                deleted = true;
                            }
                        }
                    }

                    if (deleted) {
                        System.out.println("remove lower degree pair:<" + dp.getKey().getKey() + "," + dp.getKey().getValue() + ">, size:" + dp.getValue().size()
                                + " --> " + neo4j.getNumberofNodes() + " " + neo4j.getNumberofEdges());
                    }
                }

                if (deleted) {
                    break;
                }
            }
            tx.success();
        }
        return deleted;
    }

    private ChainList FindCandidateChains(ArrayList<Long> deleted_candidate_rel_ids) {
        ChainList result = new ChainList();
        try (Transaction tx = this.graphdb.beginTx()) {
            for (long rid : deleted_candidate_rel_ids) {
                Relationship r = this.graphdb.getRelationshipById(rid);
                System.out.println(r);
                ChainList rList = findRelsInSameChain(r);
                System.out.println("final candidate list:");
                System.out.print(rList);
                System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@2");
                if (rList.size != 0) {
                    result.addAllNotOver(rList);
//                    result.addAll(rList);
                }
            }
            tx.success();
        }
        return result;
    }


    private ChainList FindNumberOfChains(Relationship r) {
        ChainList result = new ChainList();
        try (Transaction tx = this.graphdb.beginTx()) {
            ChainList clist = new ChainList();
            for (Chain c : this.chainsInGraph) {
                clist.add(c);
            }

            ChainList r_list = clist.findChainsByRelationShip(r.getStartNodeId(), r.getEndNodeId());
            result.addAll(r_list);
            ChainList reverse_r_list = clist.findChainsByRelationShip(r.getEndNodeId(), r.getStartNodeId());
            result.addAll(reverse_r_list);
            tx.success();
        }
        return result;
    }

    private ChainList findRelsInSameChain(Relationship r) {
        ChainList result = new ChainList();
        try (Transaction tx = this.graphdb.beginTx()) {
            ChainList clist = new ChainList();
            for (Chain c : this.chainsInGraph) {
                if (c.nodeList.contains(r.getStartNodeId()) || c.nodeList.contains(r.getEndNodeId())) {
                    System.out.println(c);
                }
                clist.add(c);
            }

            System.out.println("========");

            ChainList r_list = clist.findChainsByRelationShip(r.getStartNodeId(), r.getEndNodeId());
            System.out.print(r_list);
            System.out.println("-------+++ r list +++--------");
            for (int i = 0; i < r_list.min_chain.size(); i++) {
                Chain min_c = r_list.min_chain.get(i);
                System.out.println(min_c);
                if (r_list.isCandidateChain(min_c)) {
                    result.add(min_c);
                }
            }
            System.out.println("-------+++ result1 +++--------");
            System.out.print(result);
            System.out.println("-------------------");
            ChainList reverse_r_list = clist.findChainsByRelationShip(r.getEndNodeId(), r.getStartNodeId());
            System.out.print(reverse_r_list);
            System.out.println("-------+++ reverse r list +++--------");
            for (int i = 0; i < reverse_r_list.min_chain.size(); i++) {
                Chain min_c = reverse_r_list.min_chain.get(i);
                System.out.println(min_c);
                if (reverse_r_list.isCandidateChain(min_c)) {
                    result.add(min_c);
                }
            }
            System.out.println("-------+++ result2 +++--------");
            System.out.print(result);
            tx.success();
        }
        return result;
    }

    private void printChianByNodes(long id) {
        System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
        for (Chain chain : this.chainsInGraph) {
            if (chain.nodeList.contains(id)) {
                System.out.println(chain);
            }
        }
        System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
    }

    private int findNumberOfchainWithNode(long id) {
        int numberOfChain = 0;
        for (Chain chain : this.chainsInGraph) {
            if (chain.nodeList.contains(id)) {
                numberOfChain++;
                System.out.println(chain);
            }
        }
        return numberOfChain;
    }

    //If the number of chain go through the nodes is same as the threshold, keep it.
    //If not, not it means the rel are safe to delete
    private HashSet<Long> saftyDeleteRels(ArrayList<Long> rels, int min_t) {
        HashSet<Long> end_nodes = new HashSet<>();
        try (Transaction tx = graphdb.beginTx()) {
            for (long rid : rels) {
                Relationship rel = graphdb.getRelationshipById(rid);
                end_nodes.add(rel.getEndNodeId());
                end_nodes.add(rel.getStartNodeId());
            }
            tx.success();
        }
        HashSet<Long> result = new HashSet<>(end_nodes);
        for (long nid : end_nodes) {
            if (this.tt.containsKey(nid) && this.tt.get(nid) != min_t) {
                result.remove(nid);
            }
        }
        return result;
    }

    private void updateCurrentBridgeComponentBFS(HashSet<Long> bridges, ArrayList<Long> deleted_edges) {
        this.bcEdges.clear();
        try (Transaction tx = graphdb.beginTx()) {
            for (long brid : bridges) {
                Relationship bridge_rel = graphdb.getRelationshipById(brid);
                Node sNode = graphdb.getRelationshipById(brid).getStartNode();
                if (isDeletedAllExceptBridge(sNode, bridge_rel, deleted_edges)) {
                    FindBCByBFS(sNode, bridge_rel, deleted_edges);
                }

                Node eNode = graphdb.getRelationshipById(brid).getEndNode();
                if (isDeletedAllExceptBridge(eNode, bridge_rel, deleted_edges)) {
                    FindBCByBFS(eNode, bridge_rel, deleted_edges);
                }
            }
            tx.success();
        }
    }

    private void FindBCByBFS(Node sNode, Relationship bridge_rel, ArrayList<Long> deleted_edges) {
        Queue<Node> q = new LinkedList<>();
        HashSet<Long> tempVisited = new HashSet<>();
        try (Transaction tx = graphdb.beginTx()) {
            q.add(sNode);
            tempVisited.add(sNode.getId());

            while (!q.isEmpty()) {
                Node v = q.remove();
//                System.out.println(v+"   ::::");
                ArrayList<Relationship> out_rels = neo4j.getoutgoingEdge(v);
                for (Relationship rel : out_rels) {
                    bcEdges.add(rel.getId());
                    Node eNode = neo4j.getoutgoingNode(rel, v);
                    boolean isde = isDeletedAllExceptBridge(eNode, bridge_rel, deleted_edges);
//                    ArrayList<Relationship> adj_edges = neo4j.getoutgoingEdge(eNode);
//                    for (Relationship outgoing_rel : adj_edges) {
//                        //if one out going edge is not the bridge, also do not need to be deleted, return false
//                        System.out.println(outgoing_rel+" "+(!deleted_edges.contains(outgoing_rel.getId()))+"  &&  "+ (bridge_rel.getId() != outgoing_rel.getId()));
//                    }
//
//                    System.out.println(eNode +"   " +isde+"    "+rel);
                    if (isde && !tempVisited.contains(eNode.getId())) {
                        q.add(eNode);
                        tempVisited.add(eNode.getId());

                    }
                }
            }
            tx.success();
        }
    }

    public boolean isDeletedAllExceptBridge(Node snode, Relationship rel, ArrayList<Long> deleted_edges) {
        boolean flag = true;
        try (Transaction tx = graphdb.beginTx()) {
            ArrayList<Relationship> adj_edges = neo4j.getoutgoingEdge(snode);
            for (Relationship outgoing_rel : adj_edges) {
                //if one out going edge is not the bridge, also do not need to be deleted, return false
                if (!deleted_edges.contains(outgoing_rel.getId()) && rel.getId() != outgoing_rel.getId()) {
                    flag = false;
                    break;
                }
            }
            tx.success();
        }
        return flag;
    }

    private void indexBuild(int currentLevel) {
        int pre_degree_num = -1;
        int post_degree_num = -1;

        boolean deleted_two_two = false;

        do {
            System.out.println("===============  level:" + (currentLevel + 1) + " ==============");

            //calculated the degree pair of current level
            String sub_db_name = graph_size + "-" + samnode_t + "-" + "Level" + currentLevel;
            neo4j = new Neo4jDB(sub_db_name);
            neo4j.startDB();
            graphdb = neo4j.graphDB;
            getDegreePairs();
            long number_n = neo4j.getNumberofNodes();
            long number_e = neo4j.getNumberofEdges();
            if (currentLevel == 0) {
                removeSingletonEdges();
            }
            neo4j.closeDB();

            pre_degree_num = getNumberOfDegreeEdge();
//            //create higher level db by copying the lower level db
            int upperLevel = currentLevel + 1;
            copyToHigherDB(currentLevel, upperLevel);
            currentLevel = upperLevel;


            //update neo4j object
            sub_db_name = graph_size + "-" + samnode_t + "-" + "Level" + currentLevel;
            neo4j = new Neo4jDB(sub_db_name);
            neo4j.startDB();
            graphdb = neo4j.graphDB;
//
////            for (int i = 0; i < 2; i++) {
//            initialzeGraphAttrs();
//            removeSingletonEdges(degree_pairs, currentLevel);
            FindTheBridges();
            deleted_two_two = removeLowerDegreePairEdges();
            getDegreePairs();
            removeSingletonEdges();
            System.out.println("-----------------------------");
////            }
            post_degree_num = getNumberOfDegreeEdge();
            System.out.println("pre :" + number_n + " " + number_e);
            System.out.println("post:" + neo4j.getNumberofNodes() + " " + neo4j.getNumberofEdges());
//            neo4j.listallEdges();
//            neo4j.listallNodes();
            neo4j.closeDB();
//        } while (pre_degree_num != post_degree_num);
        } while (deleted_two_two);

    }

    private void displayDegreePair(int num) {
        if (num == -1) {
            num = degree_pairs.size();
        }
        int k = 0;
        for (Map.Entry<Pair<Integer, Integer>, ArrayList<Long>> dp : this.degree_pairs.entrySet()) {
            System.out.println(dp.getKey().getKey() + "  " + dp.getKey().getValue() + "  " + dp.getValue().size());
            k++;
            if (k == num) {
                break;
            }
        }
        System.out.println("=======");
    }

    private boolean removeLowerDegreePairEdges() {
        int i = 0;
        boolean deleted = false;
        try (Transaction tx = graphdb.beginTx()) {
            for (Map.Entry<Pair<Integer, Integer>, ArrayList<Long>> dp : this.degree_pairs.entrySet()) {
                if (dp.getKey().getKey() == 2 && dp.getKey().getValue() == 2) {
                    for (long rel : dp.getValue()) {
                        if (!this.bridges.contains(rel)) {
                            Relationship r = graphdb.getRelationshipById(rel);
                            Node sNode = r.getStartNode();
                            Node eNode = r.getEndNode();
//                        System.out.println("remove lower degree pair:<" + dp.getKey().getKey() + "," + dp.getKey().getValue() + "> --- "+r);

                            r.delete();
                            if (sNode.getDegree(Direction.BOTH) == 0) {
                                sNode.delete();
                            }
                            if (eNode.getDegree(Direction.BOTH) == 0) {
                                eNode.delete();
                            }
                            deleted = true;
                        }
                    }
//                    System.out.println("remove lower degree pair:<" + dp.getKey().getKey() + "," + dp.getKey().getValue() + ">, size:" + dp.getValue().size());
                }

                if (!deleted && (dp.getKey().getKey() == 2 && dp.getKey().getValue() == 3) || (dp.getKey().getKey() == 3 && dp.getKey().getValue() == 2)) {
                    for (long rel : dp.getValue()) {
                        System.out.println(graphdb.getRelationshipById(rel));
                        if (!this.bridges.contains(rel)) {
                            Relationship r = graphdb.getRelationshipById(rel);
                            Node sNode = r.getStartNode();
                            Node eNode = r.getEndNode();
//                            System.out.println("remove lower degree pair:<" + dp.getKey().getKey() + "," + dp.getKey().getValue() + "> --- " + r);

                            r.delete();
                            if (sNode.getDegree(Direction.BOTH) == 0) {
                                sNode.delete();
                            }
                            if (eNode.getDegree(Direction.BOTH) == 0) {
                                eNode.delete();
                            }
                            deleted = true;
                        }
                    }

                }


                if (!deleted && (dp.getKey().getKey() == 2 && dp.getKey().getValue() == 4) || (dp.getKey().getKey() == 4 && dp.getKey().getValue() == 2)) {
                    for (long rel : dp.getValue()) {
                        System.out.println(graphdb.getRelationshipById(rel));
                        if (!this.bridges.contains(rel)) {
                            Relationship r = graphdb.getRelationshipById(rel);
                            Node sNode = r.getStartNode();
                            Node eNode = r.getEndNode();
                            System.out.println("remove lower degree pair:<" + dp.getKey().getKey() + "," + dp.getKey().getValue() + "> --- " + r);

                            r.delete();
                            if (sNode.getDegree(Direction.BOTH) == 0) {
                                sNode.delete();
                            }
                            if (eNode.getDegree(Direction.BOTH) == 0) {
                                eNode.delete();
                            }
                            deleted = true;
                        }
                    }

                }


                if (!deleted && (dp.getKey().getKey() == 3 && dp.getKey().getValue() == 3)) {
                    for (long rel : dp.getValue()) {
                        System.out.println(graphdb.getRelationshipById(rel));
                        if (!this.bridges.contains(rel)) {
                            Relationship r = graphdb.getRelationshipById(rel);
                            Node sNode = r.getStartNode();
                            Node eNode = r.getEndNode();
                            System.out.println("remove lower degree pair:<" + dp.getKey().getKey() + "," + dp.getKey().getValue() + "> --- " + r);

                            r.delete();
                            if (sNode.getDegree(Direction.BOTH) == 0) {
                                sNode.delete();
                            }
                            if (eNode.getDegree(Direction.BOTH) == 0) {
                                eNode.delete();
                            }
                            deleted = true;
                        }
                    }

                }

                if (i == 0) {
                    break;
                }
            }
            tx.success();
        }
        return deleted;
    }

    private int FindTheBridges() {
        tt.clear();
        this.chainsInGraph.clear();
        this.numberofchains = 0;
        int cc_num_node = DFS();
//        System.out.println("================ dfs =======================");
//        System.out.println("=number of chains:" + this.numberofchains + "    ===================");
//        System.out.println("=number of nodes :" + neo4j.getNumberofNodes() + "    ===================");
        return cc_num_node;
    }

    private void removeSingletonEdges() {
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

                            r.delete();
                            if (sNode.getDegree(Direction.BOTH) == 0) {
                                sNode.delete();
//                                System.out.println("deleted node " + sNode);
                            }
                            if (eNode.getDegree(Direction.BOTH) == 0) {
                                eNode.delete();
//                                System.out.println("deleted node " + eNode);
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
//        neo4j.closeDB();

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
     * Copy the graph from the src_level to the dest_level, the dest level graph used to shrink and build index on upper level.
     *
     * @param src_level
     * @param dest_level
     */
    private void copyToHigherDB(int src_level, int dest_level) {
        String dest_db_name = graph_size + "-" + samnode_t + "-" + "Level" + dest_level;
        String src_db_name = graph_size + "-" + samnode_t + "-" + "Level" + src_level;
//        System.out.println(prop.params.get("neo4jdb")+"/"+src_db_name);
//        System.out.println(prop.params.get("neo4jdb")+"/"+dest_db_name);
        File src_db_folder = new File(prop.params.get("neo4jdb") + "/" + src_db_name);
        File dest_db_folder = new File(prop.params.get("neo4jdb") + "/" + dest_db_name);
        try {
            if (dest_db_folder.exists()) {
                FileUtils.deleteDirectory(dest_db_folder);
//                System.out.println("deleted existed upper graph db folder:" + dest_db_name);
            }
            FileUtils.copyDirectory(src_db_folder, dest_db_folder);
//            System.out.println("Copied from source db " + src_db_name + " to the upper db " + dest_db_name);

        } catch (IOException e) {
            e.printStackTrace();
        }


        //re-fresh the dfs related properties
//        Neo4jDB neo4j = new Neo4jDB(dest_db_name);
//        neo4j.startDB();
//        graphdb = neo4j.graphDB;
//        neo4j.closeDB();

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

    /**
     * Calculated the degree pair of each edge,
     * one distinct degree pair p contains the list of the edges whose degree pair of the start node and end node is equal to the given key p
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
//                        System.out.println(rel);
                    }
                }
            }

//            System.out.println("number of the node in connected component: " + dfs_order);
            cc_num_node = dfs_order;
            //mark the edges in the dfs tree
            for (long i = 0; i < dfs_order; i++) {
                Node v = graphdb.findNode(BNode.BusNode, "dfs_order", i);
                long v_from_rel_id = (long) v.getProperty("fromRelID");
//                System.out.println(v);
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

//                long v_from_rel_id = (long) v.getProperty("fromRelID");
//                if (v.getId() == 453 || v.getId() == 454 || v.getId() == 571 || v.getId() == 572) {
//                    System.out.println("to" + v + ": from link " + v_from_rel_id);
//                }


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
            } else {
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


    public int getNumberOfDegreeEdge() {
        int result = 0;
        for (Map.Entry<Pair<Integer, Integer>, ArrayList<Long>> dp : this.degree_pairs.entrySet()) {
            result += dp.getValue().size();
        }
        return result;

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