package v5LinkedList;

import DataStructure.LinkedList;
import DataStructure.ListNode;
import DataStructure.RelationshipExt;
import Neo4jTools.Neo4jDB;
import configurations.ProgramProperty;
import javafx.util.Pair;
import org.neo4j.graphdb.*;

import java.util.*;

public class SpanningTree {
    public Neo4jDB neo4j = null;
    public HashSet<Long> SpTree; // the id of the edges that belongs to the Spanning tree
    public HashSet<Long> N_nodes; // the id of the nodes in the spanning tree, its same as the nodes of the graph
    public boolean needToScanETTree = false;
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

    HashMap<Long, ListNode<RelationshipExt>> nodeFirstOccurrences = new HashMap<>();
//    HashMap<Long, ListNode<RelationshipExt>> nodeLastOccurrences = new HashMap<>();

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

    public SpanningTree(SpanningTree left_sub_tree, SpanningTree right_sub_tree) {
        this.neo4j = left_sub_tree.neo4j;

        SpTree = new HashSet<>();
        this.SpTree.addAll(left_sub_tree.SpTree);
        this.SpTree.addAll(right_sub_tree.SpTree);

        N_nodes = new HashSet<>();
        this.N_nodes.addAll(left_sub_tree.N_nodes);
        this.N_nodes.addAll(right_sub_tree.N_nodes);

        this.ettree = new LinkedList<>();
        this.appendTree(left_sub_tree);
        this.appendTree(right_sub_tree);

        this.N = N_nodes.size();
        this.E = SpTree.size();

        if (N == 1) {
            this.isSingle = true;
            this.isEmpty = false;
        } else if (N == 0) {
            this.isSingle = false;
            this.isEmpty = true;
        } else {
            this.isSingle = false;
            this.isEmpty = false;
        }
    }

    public SpanningTree(SpanningTree other_tree) {
        this.neo4j = other_tree.neo4j;

        SpTree = new HashSet<>(other_tree.SpTree);

        N_nodes = new HashSet<>(other_tree.N_nodes);

        this.ettree = new LinkedList<>();
        this.appendTree(other_tree);
        this.N = N_nodes.size();
        this.E = SpTree.size();

        if (N == 1) {
            this.isSingle = true;
            this.isEmpty = false;
        } else if (N == 0) {
            this.isSingle = false;
            this.isEmpty = true;
        } else {
            this.isSingle = false;
            this.isEmpty = false;
        }
    }

    private void appendTree(SpanningTree other_tree) {
        if (!other_tree.isSingle && !other_tree.isEmpty) {
            ListNode<RelationshipExt> current = other_tree.ettree.head;
            while (current != other_tree.ettree.tail) {
                ListNode<RelationshipExt> node = new ListNode<>(current.data);
                this.ettree.append(node);

                long edge_id = node.data.relationship.getId();
                long start_node_id = node.data.start_id;

                if (this.firstOccurrences.containsKey(node.data.relationship.getId())) {
                    this.lastOccurrences.put(edge_id, node);
                } else {
                    this.firstOccurrences.put(edge_id, node);
                }

                if (!this.nodeFirstOccurrences.containsKey(start_node_id)) {
//                    this.nodeLastOccurrences.put(start_node_id, node);
//                } else {
                    this.nodeFirstOccurrences.put(start_node_id, node);
                }

                current = current.next;
            }

            ListNode<RelationshipExt> node = new ListNode<>(current.data);
            long edge_id = node.data.relationship.getId();
            long start_node_id = node.data.start_id;

            if (this.firstOccurrences.containsKey(node.data.relationship.getId())) {
                this.lastOccurrences.put(edge_id, node);
            } else {
                this.firstOccurrences.put(edge_id, node);
            }

            if (!this.nodeFirstOccurrences.containsKey(start_node_id)) {
//                this.nodeLastOccurrences.put(start_node_id, node);
//            } else {
                this.nodeFirstOccurrences.put(start_node_id, node);
            }
            this.ettree.append(node);
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

        this.N = (int) nn;
        this.E = (int) (nn - 1);

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
        FindEulerTourStringWiki();
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


    public void FindEulerTourStringWiki() {
        HashMap<Pair<Long, Long>, Long> edge_id_mapping_List = new HashMap<>();
        List<Pair<Long, Long>> edgeList; //sorted <start_node_id, end_node_id> mapping to the relation id
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
//            System.out.println("number of node: get from neo4j object " + this.neo4j.getNumberofNodes());
//            System.out.println("number of edgs:  " + this.SpTree.size());

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

//            System.out.println(first.size() + " + " + next.size() + " = " + this.SpTree.size() * 2 + "  " + (first.size() + next.size() == this.SpTree.size() * 2 ? "Yes" : "No"));

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

                if (!this.nodeFirstOccurrences.containsKey(currentEdge.getKey())) {
//                    this.nodeLastOccurrences.put(currentEdge.getKey(), node);
//                } else {
                    this.nodeFirstOccurrences.put(currentEdge.getKey(), node);
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

//        System.out.println(left_tree_empty + "  " + middle_tree_empty + "  " + right_tree_empty);

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
        } else if (left_tree_empty && !middle_tree_empty && right_tree_empty) {
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

                middle_sub_tree.initializedAsSingleTree(f_p.data.end_id);

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


    //Todo: the performance can be improved, for example, used the original spanning tree information to update the splitted trees.
    public void etTreeUpdateInformation() {
        if (isSingle || isEmpty) {
            return;
        }

        clearData();

        int counter = 0;
        if (!ettree.isEmpty()) {
            ListNode<RelationshipExt> current = ettree.head;
            while (current != ettree.tail) {

                long edge_id = current.data.rel_id;
                long start_node_id = current.data.start_id;

                this.SpTree.add(edge_id);
                this.N_nodes.add(current.data.start_id);
                this.N_nodes.add(current.data.end_id);

                if (this.firstOccurrences.containsKey(current.data.rel_id)) {
                    this.lastOccurrences.put(edge_id, current);
                } else {
                    this.firstOccurrences.put(edge_id, current);
                }

                if (!this.nodeFirstOccurrences.containsKey(start_node_id)) {
                    this.nodeFirstOccurrences.put(start_node_id, current);
                }

                current = current.next;
                counter++;
            }
            counter++;

            long edge_id = current.data.relationship.getId();

            this.SpTree.add(edge_id);
            this.N_nodes.add(current.data.start_id);
            this.N_nodes.add(current.data.end_id);
            long start_node_id = current.data.start_id;

            this.E = SpTree.size();
            this.N = N_nodes.size();

            if (this.firstOccurrences.containsKey(edge_id)) {
                this.lastOccurrences.put(edge_id, current);
            } else {
                this.firstOccurrences.put(edge_id, current);
            }

            if (!this.nodeFirstOccurrences.containsKey(start_node_id)) {
                this.nodeFirstOccurrences.put(start_node_id, current);
            }
        }

        ettree.n = counter;
    }

    private void clearData() {
        this.nodeFirstOccurrences.clear();
//        this.nodeLastOccurrences.clear();
        this.firstOccurrences.clear();
        this.lastOccurrences.clear();
        this.SpTree.clear();
        this.N_nodes.clear();
    }


    public boolean hasEdge(Relationship r) {
        return this.SpTree.contains(r.getId());
    }

    public void initializedAsSingleTree(long nodeid) {
        clearData();

        this.N_nodes.add(nodeid);
        this.N = 1;
        this.E = 0;
        this.isSingle = true;
        this.isEmpty = false;
        this.ettree = new LinkedList<>();

    }


    public void initializedSingleEdge(Relationship next_level_rel) {
        clearData();

        this.N_nodes.add(next_level_rel.getStartNodeId());
        this.N_nodes.add(next_level_rel.getEndNodeId());
        this.SpTree.add(next_level_rel.getId());

        this.N = 2;
        this.E = 1;

        this.isSingle = false;
        this.isEmpty = false;
        this.ettree = new LinkedList<>();

        int start_id = (int) next_level_rel.getStartNodeId();
        int end_id = (int) next_level_rel.getEndNodeId();

        RelationshipExt edge = new RelationshipExt(next_level_rel, start_id, end_id);
        ListNode<RelationshipExt> node = new ListNode<>(edge);
        this.ettree.append(node);

        RelationshipExt back_edge = new RelationshipExt(next_level_rel, end_id, start_id);
        ListNode<RelationshipExt> back_node = new ListNode<>(back_edge);
        this.ettree.append(back_node);

        firstOccurrences.put(next_level_rel.getId(), node);
        lastOccurrences.put(next_level_rel.getId(), back_node);
        nodeFirstOccurrences.put((long) start_id, node);
//        nodeLastOccurrences.put((long) start_id, back_node);

    }

    private void initializedAsEmptyTree() {
        clearData();

        this.N = 0;
        this.E = 0;
        this.isSingle = false;
        this.isEmpty = true;
    }

    public void increaseEdgeLevel(int current_level) {
        try (Transaction tx = this.neo4j.graphDB.beginTx()) {
            for (long rel_id : SpTree) {
                Relationship rel = this.neo4j.graphDB.getRelationshipById(rel_id);
                int c_level = (int) rel.getProperty("level");
                if (c_level == current_level) {
                    rel.setProperty("level", current_level + 1);
                }
            }
            tx.success();
        }
    }

    public void reroot(long new_root_id) {
        if (!this.isSingle && !this.isEmpty) {
            ListNode<RelationshipExt> f_p = nodeFirstOccurrences.get(new_root_id);
            this.ettree.tail.next = this.ettree.head;
            this.ettree.head.prev = this.ettree.tail;
            this.ettree.tail = f_p.prev;
            this.ettree.head = f_p;
            this.ettree.head.prev = null;
            this.ettree.tail.next = null;
        }
    }


    public void printETTree() {
        if (!ettree.isEmpty()) {
            ListNode<RelationshipExt> current = ettree.head;
            while (current != ettree.tail) {
                System.out.println(current.data);
                current = current.next;
            }
            System.out.println(current.data);
        }
    }

    public void printETTreeLast() {
        if (!ettree.isEmpty()) {
            ListNode<RelationshipExt> current = ettree.head;
            while (current != ettree.tail) {
                current = current.next;
            }
            System.out.println(current.data);
        }
    }

    public void printETTree(int limit) {
        if (!ettree.isEmpty()) {
            int counter = 1;
            ListNode<RelationshipExt> current = ettree.head;
            while (current != ettree.tail) {
                if (counter <= limit) {
                    System.out.println(current.data);
                }
                current = current.next;
                counter++;
            }

            if (counter <= limit) {
                System.out.println(current.data);
            }
        } else if (this.isSingle) {
            System.out.println("It's a single tree with the node " + this.N_nodes.iterator().next());
        } else if (this.isEmpty) {
            System.out.println("it's a empty tree !!!!!!");
        }
    }

    public boolean hasEdge(long rel_id) {
        return SpTree.contains(rel_id);
    }

    public int removeSingleEdge(long id) {

        ListNode<RelationshipExt> f_p = firstOccurrences.get(id);
        ListNode<RelationshipExt> l_p = lastOccurrences.get(id);

        int remove_case = 0;

        try {
            if (f_p == ettree.head && l_p == ettree.tail) {
                if (f_p.next == l_p && l_p.prev == f_p) {
                    ettree.head = ettree.tail = null;
                    remove_case = 1;
                } else if (f_p.next != l_p && l_p.prev != f_p) {
                    ettree.head = f_p.next;
                    ettree.head.prev = null;
                    ettree.tail = l_p.prev;
                    ettree.tail.next = null;
                    remove_case = 2;
                }
            } else {
                if (f_p != ettree.head && l_p != ettree.tail) {
                    f_p.prev.next = l_p.next;
                    l_p.next.prev = f_p.prev;
                    remove_case = 3;
                } else if (f_p == ettree.head && l_p != ettree.tail) {
                    ettree.head = l_p.next;
                    ettree.head.prev = null;
                    remove_case = 4;
                } else if (f_p != ettree.head && l_p == ettree.tail) {
                    ettree.tail = f_p.prev;
                    ettree.tail.next = null;
                    remove_case = 5;
                }
            }

            etTreeUpdateInformation();

        } catch (Exception e) {
            System.out.println("error when remove the single edges " + id + "    " + remove_case);
            e.printStackTrace();
        }
        return remove_case;
    }
}

