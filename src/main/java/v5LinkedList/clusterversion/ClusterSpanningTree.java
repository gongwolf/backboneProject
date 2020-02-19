package v5LinkedList.clusterversion;

import DataStructure.LinkedList;
import DataStructure.ListNode;
import DataStructure.RelationshipExt;
import Neo4jTools.Neo4jDB;
import configurations.ProgramProperty;
import javafx.util.Pair;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import v5LinkedList.UFnode;

import java.util.*;

public class ClusterSpanningTree {

    public Neo4jDB neo4j = null;
    public HashSet<Long> SpTree; // the id of the edges that belongs to the Spanning tree
    public HashSet<Long> N_nodes; // the id of the nodes in the spanning tree, its same as the nodes of the graph
    LinkedList<RelationshipExt> ettree;

    int E = 0; // number of edges
    int N = 0; // number of nodes

    public ProgramProperty prop = new ProgramProperty();
    int connect_component_number = 0; // number of the connect component found in the graph
    HashMap<Long, UFnode> unionfind = new HashMap<>();
    boolean isSingle = false;
    boolean isEmpty = false;

    HashMap<Long, ListNode<RelationshipExt>> firstOccurrences = new HashMap();
    HashMap<Long, ListNode<RelationshipExt>> lastOccurrences = new HashMap();

    HashMap<Long, ListNode<RelationshipExt>> nodeFirstOccurrences = new HashMap<>();

    HashSet<Long> rels = new HashSet<>();

    public ClusterSpanningTree(Neo4jDB neo4j, boolean init, HashSet<Long> node_list) {
        this.neo4j = neo4j;
        SpTree = new HashSet<>();
        N_nodes = new HashSet<>(node_list);
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
        System.out.println(neo4j.DB_PATH);
        long nn = this.N_nodes.size();
        this.rels = new HashSet<>(neo4j.getEdges(this.N_nodes));
        System.out.println("number of nodes :" + nn + "   number of edges :" + rels.size());

        this.N = (int) nn;
        this.E = (int) (nn - 1);

        // At the beginning, the number of connected components is the number of nodes.
        // Each node is a connected component.
        this.connect_component_number = N;

        for (long n_id : this.N_nodes) {
            UFnode unode = new UFnode(n_id);
            unionfind.put(n_id, unode);
        }
    }


    public String EulerTourStringWiki() {
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
        try (Transaction tx = this.neo4j.graphDB.beginTx()) {
            SpTree = new HashSet<>();
            int e = 0;
            Iterator<Long> rel_iterator = rels.iterator();

            while (e < N - 1 && rel_iterator.hasNext()) {
                Relationship rel = this.neo4j.graphDB.getRelationshipById(rel_iterator.next());
                long src_id = rel.getStartNodeId();
                long dest_id = rel.getEndNodeId();
                long src_root = find(src_id);
                long dest_root = find(dest_id);
                if (src_root != dest_root) {
                    SpTree.add(rel.getId());
                    e++;
                    union(src_root, dest_root);
                    connect_component_number--;
                }
            }

            tx.success();
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
}
