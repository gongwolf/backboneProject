package v5LinkedList;

import DataStructure.LinkedList;
import DataStructure.ListNode;
import DataStructure.RelationshipExt;
import Neo4jTools.Line;
import Neo4jTools.Neo4jDB;
import configurations.ProgramProperty;
import javafx.util.Pair;
import org.neo4j.graphdb.*;

import java.util.*;

public class SpanningTree {
    public Neo4jDB neo4j = null;
    public HashSet<Long> SpTree; // the id of the edges that belongs to the Spanning tree
    public HashSet<Long> N_nodes; // the id of the nodes in the spanning tree, its same as the nodes of the graph
    int E = 0; // number of edges
    int N = 0; // number of nodes
    String DBPath;
    public ProgramProperty prop = new ProgramProperty();
    int connect_component_number = 0; // number of the connect component found in the graph
    HashMap<Long, UFnode> unionfind = new HashMap<>();
    boolean isSingle = false;
    boolean isEmpty = false;

    LinkedList<RelationshipExt> ettree;

    HashMap<Long, ListNode<RelationshipExt>> firstOccurrences = new HashMap();
    HashMap<Long, ListNode<RelationshipExt>> lastOccurrences = new HashMap();

    HashSet<Relationship> hash_rel = new HashSet<>();


    /**
     * @param neo4j
     * @param init  if it is true, call the initialization() function that gets the information by reading the graph database from disk;
     */
    public SpanningTree(Neo4jDB neo4j, boolean init) {
        this.neo4j = neo4j;
        SpTree = new HashSet<>();
        N_nodes = new HashSet<>();
        ettree = new LinkedList<>();

        if (init) {
            initialization();
        }
    }


    /**
     * Initialization:
     * Get the number of edges
     * Get the number of nodes
     * The init number of connect components is the number of the nodes
     * each node whose root is the node id
     */
    private void initialization() {
        this.N_nodes = new HashSet<>();

        if (neo4j == null) {
            DBPath = prop.params.get("neo4jdb");
//            String sub_db_name = graphsize + "_" + degree + "_" + dimension + "_Level" + level;
            String sub_db_name = "testRandomGraph_14_4";
            neo4j = new Neo4jDB(sub_db_name);
            System.out.println("connected to db " + neo4j.DB_PATH);
            neo4j.startDB(false);
        }

        System.out.println(neo4j.DB_PATH);
        long nn = neo4j.getNumberofNodes();
        long en = neo4j.getNumberofEdges();
        System.out.println("number of nodes :" + nn + "   number of edges :" + en);

        this.E = (int) en;
        this.N = (int) nn;

        // At the beginning, the number of connected components is the number of nodes.
        // Each node is a connected component.
        this.connect_component_number = N;


        try (Transaction tx = neo4j.graphDB.beginTx()) {
            ResourceIterable<Node> allNodesIterable = neo4j.graphDB.getAllNodes();
            for (Node n : allNodesIterable) {
                long node_id = n.getId();
                UFnode unode = new UFnode(node_id);
                unionfind.put(node_id, unode);
            }

            ResourceIterable<Relationship> allRelsIterable = neo4j.graphDB.getAllRelationships();
            for (Relationship r : allRelsIterable) {
                r.setProperty("level", 0); //initialize the edge level to be 0
                hash_rel.add(r);
            }
            tx.success();
        }
    }

    public String EulerTourStringWiki(int level) {
        KruskalMST();
        System.out.println("number of nodes in the sp tree :" + this.N_nodes.size());
        System.out.println("number of component :" + this.connect_component_number);
        System.out.println("number of rels in sp :" + this.SpTree.size());
        System.out.println("Finished the calling of the KruskalMST() function");
        long start_euler_finding = System.currentTimeMillis();
        FindEulerTourStringWiki(level);
        long end_euler_finding = System.currentTimeMillis();
        System.out.println("There are " + this.ettree.n + " extended Relationship in the linked list et tree");
        System.out.println("Finished the calling of the elurtourString() function in " + (end_euler_finding - start_euler_finding) + " ms");
        System.out.println("======================================================================================");
        return null;
    }

    /**
     * Get the spanning tree of the graph.
     * Get number of components
     * Get the relationships that consists of the spanning tree
     */
    public void KruskalMST() {
        SpTree = new HashSet<>();
        int e = 0;
//        int i = 0;
        Iterator<Relationship> rel_iterator = hash_rel.iterator();

        while (e < N - 1 && rel_iterator.hasNext()) {
//            Relationship rel = rels[i++]; //scan each edge
            Relationship rel = rel_iterator.next();
            long src_id = rel.getStartNodeId();
            long dest_id = rel.getEndNodeId();
            long src_root = find(src_id);
            long dest_root = find(dest_id);
            if (src_root != dest_root) {
                SpTree.add(rel.getId());
                updateNodesIDInformation(rel);
                e++;
                union(src_root, dest_root);
                connect_component_number--;
            }
        }
    }

    public void updateNodesIDInformation(Relationship rel) {
        if (rel != null) {
            Long sid = rel.getStartNodeId();
            Long eid = rel.getEndNodeId();

            if (!N_nodes.contains(sid)) {
                N_nodes.add(sid);
            }

            if (!N_nodes.contains(eid)) {
                N_nodes.add(eid);
            }
        }
    }

    /**
     * Union the lower rank component to the higher rank component
     * If the rank of two components are same, union the dest component to the src component.
     * Then increase the rank ot the src component.
     *
     * @param src_root  the root of src node in UF structure
     * @param dest_root the root of dest node in UF structure
     */
    private void union(long src_root, long dest_root) {
        if (unionfind.get(src_root).rank < unionfind.get(dest_root).rank) {
            unionfind.get(src_root).parentID = dest_root;
            unionfind.get(dest_root).size += unionfind.get(src_root).size;
        } else if (unionfind.get(src_root).rank > unionfind.get(dest_root).rank) {
            unionfind.get(dest_root).parentID = src_root;
            unionfind.get(src_root).size += unionfind.get(dest_root).size;
        } else {
            unionfind.get(dest_root).parentID = src_root;
            unionfind.get(src_root).rank++;
            unionfind.get(src_root).size += unionfind.get(dest_root).size;
        }
    }

    /**
     * Find the root of the node, only the root node's id is equal to its node id.
     *
     * @param src_id the node id
     * @return the root id of src_id
     */
    private long find(long src_id) {
        while (unionfind.get(src_id).parentID != src_id) {
            src_id = unionfind.get(src_id).parentID;
        }
        return src_id;
    }


    private void FindEulerTourStringWiki(int level) {
        HashMap<Pair<Long, Long>, Long> edge_id_mapping_List = new HashMap<>();
        List<Pair<Long, Long>> edgeList;
        Comparator<Pair<Long, Long>> valueComparator = (o1, o2) -> {
            if (o1.getKey() == o2.getKey()) {
                return (int) (o1.getValue() - o2.getValue());
            } else {
                return (int) (o1.getKey() - o2.getKey());
            }
        };

        HashMap<Long, Pair<Long, Long>> first = new HashMap<>(); // first(u) = (u,v)
        HashMap<Pair<Long, Long>, Pair<Long, Long>> next = new HashMap<>(); // next(x,y) = (u,v)


        try (Transaction tx = this.neo4j.graphDB.beginTx()) {
            System.out.println("number of node: get from neo4j object " + this.neo4j.getNumberofNodes());
            System.out.println("number of edgs:  " + this.SpTree.size());

            for (long rel_id : this.SpTree) {
                Relationship rel = neo4j.graphDB.getRelationshipById(rel_id);
                long sid = rel.getStartNodeId();
                long did = rel.getEndNodeId();

                Pair<Long, Long> key = new Pair<>(sid, did);
                Pair<Long, Long> reverse_key = new Pair<>(did, sid);

                edge_id_mapping_List.put(key, rel.getId());
                edge_id_mapping_List.put(reverse_key, rel.getId());
            }

            //the edge list and order the edge list
            edgeList = new ArrayList<>(edge_id_mapping_List.keySet());
            Collections.sort(edgeList, valueComparator);

            for (Pair<Long, Long> edge_pair : edgeList) {
                long sid = edge_pair.getKey();
                if (first.containsKey(sid)) {
                    Pair key_p = first.get(sid);
                    /** find the right previous key for current edge**/
                    while (next.containsKey(key_p)) {
                        key_p = next.get(key_p);
                    }
                    next.put(key_p, edge_pair);
                } else {
                    first.put(sid, edge_pair);
                }
            }

            System.out.println(first.size() + " + " + next.size() + " = " + this.SpTree.size() * 2 + "  " + (first.size() + next.size() == this.SpTree.size() * 2 ? "Yes" : "No"));

            Pair<Long, Long> firstEdge = edgeList.get(0);
            Pair<Long, Long> currentEdge = edgeList.get(0);
            do {
                Relationship rel = neo4j.graphDB.getRelationshipById(edge_id_mapping_List.get(currentEdge));

                RelationshipExt iter_edge = new RelationshipExt(rel, currentEdge.getKey().intValue(), currentEdge.getValue().intValue());
                ListNode<RelationshipExt> node = new ListNode<>(iter_edge);
                ettree.append(node);
                if (this.firstOccurrences.containsKey(rel.getId())) {
                    this.lastOccurrences.put(rel.getId(), node);
                } else {
                    this.firstOccurrences.put(rel.getId(), node);
                }


                Pair<Long, Long> reverse = new Pair<>(currentEdge.getValue(), currentEdge.getKey());
                if (next.containsKey(reverse)) {
                    currentEdge = next.get(reverse);
                } else {
                    currentEdge = first.get(reverse.getKey());
                }

            } while (firstEdge != currentEdge);
            tx.success();
        }
    }


    //Todo: Do merge the left and the right sub-tree, return trees of the tree, find the replacement edge in (left and right) together vs middle tree. So, it will not change the structure of the tree before finding a replacement edge.
    public int split(Relationship r, SpanningTree[] splittedTrees) {

        int case_number = -1;

        SpanningTree left_sub_tree = splittedTrees[0];
        SpanningTree middle_sub_tree = splittedTrees[1];
        SpanningTree right_sub_tree = splittedTrees[2];

        ListNode<RelationshipExt> f_p = firstOccurrences.get(r.getId());
        ListNode<RelationshipExt> l_p = lastOccurrences.get(r.getId());

        boolean left_tree_empty = (this.ettree.head == f_p);
        boolean middle_tree_empty = (f_p.next == l_p && l_p.prev == f_p);
        boolean right_tree_empty = (this.ettree.tail == l_p);

        System.out.println("left tree empty ? " + left_tree_empty + ", middle tree empty ? " + middle_tree_empty + ", right tree empty ? " + right_tree_empty);

        if (left_tree_empty && middle_tree_empty && right_tree_empty) {

            left_sub_tree.initializedAsSingleTree(r.getStartNodeId());
            middle_sub_tree.initializedAsSingleTree(r.getEndNodeId());
            right_sub_tree.initializedAsEmptyTree();

            case_number = 1;

        } else if (!left_tree_empty && middle_tree_empty && !right_tree_empty) {
            left_sub_tree.ettree.head = this.ettree.head;
            left_sub_tree.ettree.tail = f_p.prev;
            left_sub_tree.etTreeUpdateInformation();

            middle_sub_tree.initializedAsSingleTree(f_p.data.end_id);

            right_sub_tree.ettree.head = l_p.next;
            right_sub_tree.ettree.tail = this.ettree.tail;
            right_sub_tree.etTreeUpdateInformation();

            case_number = 2;
        } else if (left_tree_empty && !middle_tree_empty && left_tree_empty) {
            left_sub_tree.initializedAsSingleTree(f_p.data.start_id);

            middle_sub_tree.ettree.head = f_p.next;
            middle_sub_tree.ettree.tail = l_p.prev;
            middle_sub_tree.etTreeUpdateInformation();

            right_sub_tree.initializedAsEmptyTree();

            case_number = 3;

        } else if (middle_tree_empty) {
            if (!left_tree_empty && right_tree_empty) {
                left_sub_tree.ettree.head = this.ettree.head;
                left_sub_tree.ettree.tail = f_p.prev;
                left_sub_tree.etTreeUpdateInformation();

                middle_sub_tree.initializedAsSingleTree(f_p.data.end_id);

                right_sub_tree.initializedAsEmptyTree();

                case_number = 4;

            } else if (left_tree_empty && !right_tree_empty) {
                left_sub_tree.initializedAsEmptyTree();

                middle_sub_tree.initializedAsSingleTree(f_p.data.start_id);

                right_sub_tree.ettree.head = l_p.next;
                right_sub_tree.ettree.tail = this.ettree.tail;
                right_sub_tree.etTreeUpdateInformation();

                case_number = 5;
            }
        } else if (!middle_tree_empty) {

            middle_sub_tree.ettree.head = f_p.next;
            middle_sub_tree.ettree.tail = l_p.prev;
            middle_sub_tree.etTreeUpdateInformation();

            if (!left_tree_empty && right_tree_empty) {
                left_sub_tree.ettree.head = this.ettree.head;
                left_sub_tree.ettree.tail = f_p.prev;
                left_sub_tree.etTreeUpdateInformation();

                right_sub_tree.initializedAsEmptyTree();

                case_number = 6;

            } else if (left_tree_empty && !right_tree_empty) {
                left_sub_tree.initializedAsEmptyTree();

                right_sub_tree.ettree.head = l_p.next;
                right_sub_tree.ettree.tail = this.ettree.tail;
                right_sub_tree.etTreeUpdateInformation();

                case_number = 7;

            } else if (!left_tree_empty && !right_tree_empty) {
                left_sub_tree.ettree.head = this.ettree.head;
                left_sub_tree.ettree.tail = f_p.prev;
                left_sub_tree.etTreeUpdateInformation();

                right_sub_tree.ettree.head = l_p.next;
                right_sub_tree.ettree.tail = this.ettree.tail;
                right_sub_tree.etTreeUpdateInformation();

                case_number = 8;
            }
        }
        return case_number;
    }

    private void initializedAsEmptyTree() {
        this.N_nodes.clear();
        this.SpTree.clear();
        this.N = 0;
        this.E = 0;
        this.isSingle = false;
        this.isEmpty = true;
    }

    //Todo: the performance can be improved, for example, used the original spanning tree information to update the splitted trees.
    public void etTreeUpdateInformation() {
        int counter = 0;
        if (!ettree.isEmpty()) {
            ListNode<RelationshipExt> current = ettree.head;
            while (current != ettree.tail) {

                long edge_id = current.data.relationship.getId();

                this.SpTree.add(edge_id);
                this.N_nodes.add(current.data.relationship.getStartNodeId());
                this.N_nodes.add(current.data.relationship.getEndNodeId());

                if (this.firstOccurrences.containsKey(current.data.relationship.getId())) {
                    this.lastOccurrences.put(edge_id, current);
                } else {
                    this.firstOccurrences.put(edge_id, current);
                }

                current = current.next;
                counter++;
            }
            counter++;

            long edge_id = current.data.relationship.getId();
            this.SpTree.add(edge_id);
            this.N_nodes.add(current.data.relationship.getStartNodeId());
            this.N_nodes.add(current.data.relationship.getEndNodeId());
            this.E = SpTree.size();
            this.N = N_nodes.size();

            if (this.firstOccurrences.containsKey(current.data.relationship.getId())) {
                this.lastOccurrences.put(edge_id, current);
            } else {
                this.firstOccurrences.put(edge_id, current);
            }
        }

        ettree.n = counter;
    }


    public boolean hasEdge(Relationship r) {
        return this.SpTree.contains(r.getId());
    }

    public void initializedAsSingleTree(long nodeid) {
        this.N_nodes.clear();
        this.N_nodes.add(nodeid);
        this.SpTree.clear();
        this.N = 1;
        this.E = 0;
        this.isSingle = true;
        this.ettree = new LinkedList<>();

    }

    public void increaseEdgeLevel() {
        try (Transaction tx = this.neo4j.graphDB.beginTx()) {
            for (long rel_id : SpTree) {
                Relationship rel = this.neo4j.graphDB.getRelationshipById(rel_id);
                int c_level = (int) rel.getProperty("level");
                rel.setProperty("level", c_level + 1);
            }
            tx.success();
        }
    }

    /**
     * Find one relationship with given level to connect current spanning tree with another spanning tree.
     *
     * @param other_tree another spanning tree
     * @param level      given level
     * @return the replacement edge, if can not find, return null.
     */
    public Relationship findReplacementEdge(SpanningTree other_tree, int level, Relationship del) {
        if (other_tree.isEmpty) {
            return null;
        }

        //Todo: Maybe can remove this condition
        Relationship rel = null;
        if (other_tree.isSingle) {
            try (Transaction tx = this.neo4j.graphDB.beginTx()) {
                long single_node_id = other_tree.N_nodes.iterator().next();
                Iterator<Relationship> rels_iter = this.neo4j.graphDB.getNodeById(single_node_id).getRelationships(Line.Linked, Direction.BOTH).iterator();
                while (rels_iter.hasNext()) {
                    Relationship next_rel = rels_iter.next();
                    int edge_level = (int) next_rel.getProperty("level");
                    if (edge_level == level && next_rel.getId() != del.getId()) {
                        if (this.N_nodes.contains(next_rel.getOtherNodeId(single_node_id))) {
                            rel = next_rel;
                            return rel;
                        } else {
                            next_rel.setProperty("level", edge_level + 1); //Increase the edge level by 1
                        }
                    }
                }
                tx.success();
            }
        } else {
            try (Transaction tx = this.neo4j.graphDB.beginTx()) {
                for (long node_id : N_nodes) {
                    Node n = this.neo4j.graphDB.getNodeById(node_id);
                    Iterator<Relationship> rels_iter = n.getRelationships(Line.Linked, Direction.BOTH).iterator();
                    while (rels_iter.hasNext()) {
                        Relationship next_rel = rels_iter.next();
                        int edge_level = (int) next_rel.getProperty("level");
                        if (edge_level == level && next_rel.getId() != del.getId()) {
                            if (other_tree.N_nodes.contains(next_rel.getOtherNodeId(node_id))) {
                                rel = next_rel;
                                return rel;
                            } else {
                                next_rel.setProperty("level", edge_level + 1); //Increase the edge level by 1
                            }
                        }
                    }
                }
                tx.success();
            }
        }
        return rel;
    }

    public void reroot(long startNodeId) {
    }

    public void removeEdge(long id) {
    }
}

class UFnode {
    long parentID;
    long nodeID;
    int rank;
    int size; //size of the subtree, include current node;

    public UFnode(long nodeID) {
        this.nodeID = nodeID;
        this.parentID = nodeID;
        this.rank = 0;
        this.size = 1;
    }

    @Override
    public String toString() {
        return "UFnode{" +
                "parentID=" + parentID +
                ", nodeID=" + nodeID +
                ", rank=" + rank +
                ", size=" + size +
                '}';
    }
}

