package v3;

import DataStructure.*;
import Neo4jTools.Neo4jDB;
import configurations.ProgramProperty;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import v3.Bag.*;

import java.util.TreeMap;

public class SpanningTree {
    public int level;
    public ProgramProperty prop = new ProgramProperty();
    int E = 0; // number of edges
    int N = 0; // number of nodes
    String DBPath;

    long graphsize;
    double samenode_t;
    int connect_component_number = 0; // number of the connect component found in the graph
    Relationship[] rels;
    UFnode unionfind[];
    //Store the relationships that consists the spanning tree.
    Relationship SpTree[];
    //the adjacent list of the spanning tree
    Bag adjList[];

    RedBlackTree rbtree = new RedBlackTree();
    public Neo4jDB neo4j = null;


    public SpanningTree(int graphsize, double samenode_t) {
        this.graphsize = graphsize;
        this.samenode_t = samenode_t;
        this.level = 0;
        initiliztion();
    }


    public SpanningTree(int graphsize, double samenode_t, int current_level) {
        this.graphsize = graphsize;
        this.samenode_t = samenode_t;
        this.level = current_level;
        initiliztion();
    }

    public SpanningTree(Neo4jDB neo4j) {
        this.neo4j = neo4j;
        initiliztion();
    }


    public static void main(String args[]) {
        SpanningTree spt = new SpanningTree(14, 0);
        spt.EulerTourString();
    }

    public String EulerTourString(TreeMap<Long, GraphNode> graph_node_spanning_rb_map) {
        KruskalMST();
        FindAdjList();
        String elurtourString = FindEulerTourString(0, graph_node_spanning_rb_map);
        return elurtourString;
    }


    public String EulerTourString() {
        KruskalMST();
        FindAdjList();
        String elurtourString = FindEulerTourString(0);
        return elurtourString;
    }


    private String FindEulerTourString(int src_id, TreeMap<Long, GraphNode> graph_node_spanning_rb_map) {
        int et_edge_id = 0;

        RelationshipExt iter_edge = adjList[src_id].getFirstUnvisitedOutgoingEdge();
        int current_start_id = -1, current_end_id = -1;
        if (iter_edge != null) {
            current_start_id = iter_edge.start_id;
            current_end_id = iter_edge.end_id;
            TNode<RelationshipExt> node = new TNode<>(et_edge_id, iter_edge);
            rbtree.insert(node);

            long nodeid = current_start_id;

            if (graph_node_spanning_rb_map.containsKey(nodeid)) {
                GraphNode grappnode = graph_node_spanning_rb_map.get(nodeid);
                grappnode.last = node; // the last pointer of the graph node is pointed to the current node
                grappnode.node = null;
                graph_node_spanning_rb_map.put((long) current_start_id, grappnode);
            } else {
                GraphNode grappnode = new GraphNode();
                grappnode.frist = grappnode.last = node;
                grappnode.node = null;
                graph_node_spanning_rb_map.put((long) current_start_id, grappnode);
            }
            updateRelationshipRBPointer(iter_edge, et_edge_id);
            et_edge_id++;
        }

        StringBuilder sb = new StringBuilder();
        StringBuilder sb1 = new StringBuilder();
        sb.append("[").append(current_start_id).append(",").append(current_end_id).append("]-");
        sb1.append(current_start_id).append(",");

        //Termination Condition
        //1) All the outgoing edges are visited.
        //2) The current_end_id is the src node
        while (adjList[src_id].getFirstUnvisitedOutgoingEdge() != null || current_end_id != src_id) {
//            System.out.println(iter_edge.relationship + "  " + current_start_id + " " + current_end_id);
            iter_edge.visited = true;
            iter_edge = getProperUnvisitedOutgoingEdge(current_end_id);
//            System.out.println("next edge: "+iter_edge.relationship + "  " + iter_edge.start_id + " " + iter_edge.end_id);
            if (iter_edge != null) {
                current_start_id = iter_edge.start_id;
                current_end_id = iter_edge.end_id;
                TNode<RelationshipExt> node = new TNode<>(et_edge_id, iter_edge);
                rbtree.insert(node);

                long nodeid = current_start_id;

                if (graph_node_spanning_rb_map.containsKey(nodeid)) {
                    GraphNode grappnode = graph_node_spanning_rb_map.get(nodeid);
                    grappnode.last = node; // the last pointer of the graph node is pointed to the current node
                    grappnode.node = null;
                    graph_node_spanning_rb_map.put((long) current_start_id, grappnode);
                } else {
                    GraphNode grappnode = new GraphNode();
                    grappnode.frist = grappnode.last = node;
                    grappnode.node = null;
                    graph_node_spanning_rb_map.put((long) current_start_id, grappnode);
                }

                updateRelationshipRBPointer(iter_edge, et_edge_id);
                et_edge_id++;

            }
//            System.out.println((adjList[src_id].getFirstUnvisitedOutgoingEdge() == null) + "   " + (current_end_id == src_id));
            sb.append("[").append(current_start_id).append(",").append(current_end_id).append("]-");
            sb1.append(current_start_id).append(",");
//            System.out.println("--------------------------------------------------------");
        }

//        System.out.println(sb.toString().substring(0,sb.length()-1));
        System.out.println(sb1.append(src_id));
        return sb.toString().substring(0, sb.length() - 1);
    }


    private String FindEulerTourString(int src_id) {
        int et_edge_id = 0;

        RelationshipExt iter_edge = adjList[src_id].getFirstUnvisitedOutgoingEdge();
        int current_start_id = -1, current_end_id = -1;
        if (iter_edge != null) {
            current_start_id = iter_edge.start_id;
            current_end_id = iter_edge.end_id;
            TNode<RelationshipExt> node = new TNode<RelationshipExt>(et_edge_id++, iter_edge);
            rbtree.insert(node);
        }

        StringBuilder sb = new StringBuilder();
        StringBuilder sb1 = new StringBuilder();
        sb.append("[").append(current_start_id).append(",").append(current_end_id).append("]-");
        sb1.append(current_start_id).append(",");

        //Termination Condition
        //1) All the outgoing edges are visited.
        //2) The current_end_id is the src node
        while (adjList[src_id].getFirstUnvisitedOutgoingEdge() != null || current_end_id != src_id) {
            System.out.println(iter_edge.relationship + "  " + current_start_id + " " + current_end_id);
            iter_edge.visited = true;
            iter_edge = getProperUnvisitedOutgoingEdge(current_end_id);
            System.out.println("next edge: " + iter_edge.relationship + "  " + iter_edge.start_id + " " + iter_edge.end_id);
            if (iter_edge != null) {
                current_start_id = iter_edge.start_id;
                current_end_id = iter_edge.end_id;
                TNode<RelationshipExt> node = new TNode<>(et_edge_id++, iter_edge);
                rbtree.insert(node);
            }
//            System.out.println((adjList[src_id].getFirstUnvisitedOutgoingEdge() == null) + "   " + (current_end_id == src_id));
            sb.append("[").append(current_start_id).append(",").append(current_end_id).append("]-");
            sb1.append(current_start_id).append(",");
            System.out.println("--------------------------------------------------------");
        }

        System.out.println(sb.toString().substring(0, sb.length() - 1));
        System.out.println(sb1.append(src_id));
        return sb.toString().substring(0, sb.length() - 1);
    }

    /**
     * First priority: unvisited edge
     * Second priority: have coming edge from other node, but no outgoing edge, which means the traverse
     * finished in the sub-tree of the current_end_id node. It goes back to the parent of the node.
     *
     * @param current_end_id: the end node id of the node which wants find the proper edge to continuous the expansion.
     * @return the proper edge needs to do the expansion in next step.
     */
    private RelationshipExt getProperUnvisitedOutgoingEdge(int current_end_id) {
        Node<RelationshipExt> current = adjList[current_end_id].first;
        RelationshipExt next_edge = new RelationshipExt();
//        System.out.println(adjList[current_end_id].size()+"  "+current_end_id);
        while (current != null) {
//            System.out.println(current);
            boolean forward_visited = current.item.visited;
            boolean backward_visited = IsBackVisited(current.item.end_id, current.item.start_id);
//            System.out.println(current.item+"    "+forward_visited+"  "+backward_visited+" ");
//            System.out.println(current.next);
            if (!forward_visited && !backward_visited) {
                next_edge = current.item;
                break;
//            } else if(forward_visited && next_edge.relationship==null && !backward_visited){
//                next_edge = current.item;
            } else if (!forward_visited && next_edge.relationship == null && backward_visited) {
                next_edge = current.item;
            }
            current = current.next;
        }
        return next_edge;

    }

    private boolean IsBackVisited(int start_id, int end_id) {
        Node<RelationshipExt> current = adjList[start_id].first;
        while (current != null) {
            if (current.item.end_id == end_id) {
                if (current.item.visited) {
                    return true;
                } else {
                    return false;
                }
            }
            current = current.next;
        }
        return false;
    }

    private void FindAdjList() {
        adjList = new Bag[N];

        for (int i = 0; i < N; i++) {
            adjList[i] = new Bag();
        }

        for (Relationship r : SpTree) {
            int src_id = (int) r.getStartNodeId();
            int dest_id = (int) r.getEndNodeId();
            //Treat the edges as the un-directional edges.
            //Add the the adj list of the source node and dest node, respectively.
            RelationshipExt rel_ext = new RelationshipExt(r, src_id, dest_id);
            adjList[src_id].add(rel_ext);
            RelationshipExt rel_ext_reverse = new RelationshipExt(r, dest_id, src_id);
            adjList[dest_id].add(rel_ext_reverse);
        }
    }

    /**
     * Initialization:
     * Get the number of edges
     * Get the number of nodes
     * The init number of connect components is the number of the nodes
     * each node whose root is the node id
     */
    private void initiliztion() {
        boolean needtoCloseDB = false;

        if (neo4j == null) {
            DBPath = prop.params.get("neo4jdb");
            String sub_db_name = graphsize + "-" + samenode_t + "-Level" + level;
            neo4j = new Neo4jDB(sub_db_name);
            System.out.println("connected to db " + neo4j.DB_PATH);
            neo4j.startDB();
            needtoCloseDB=true;
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


        rels = new Relationship[E];
        unionfind = new UFnode[N];
        int i = 0;
        for (; i < N; i++) {
            UFnode unode = new UFnode(i);
            unionfind[i] = unode;
        }

        i = 0;
        try (Transaction tx = neo4j.graphDB.beginTx()) {
            ResourceIterable<Relationship> allRelsIterable = neo4j.graphDB.getAllRelationships();
            for (Relationship r : allRelsIterable) {
                r.setProperty("level", 0); //initialize the edge level to be 0
                rels[i++] = r;
            }
            tx.success();
        }
        rbtree = new RedBlackTree();


        if (needtoCloseDB) {
            neo4j.closeDB();
        }
    }


    private void updateRelationshipRBPointer(RelationshipExt iter_edge, int et_edge_id) {
        boolean needtoCloseDB = false;
        if (neo4j == null) {
            DBPath = prop.params.get("neo4jdb");
            String sub_db_name = graphsize + "-" + samenode_t + "-Level" + level;
            neo4j = new Neo4jDB(sub_db_name);
            neo4j.startDB();
            needtoCloseDB = true;
            System.out.println("connected to db "+neo4j.DB_PATH);
        }

        try (Transaction tx = neo4j.graphDB.beginTx()) {
            Relationship r = iter_edge.relationship;
            System.out.print(r);
            if (!r.hasProperty("pFirstID")) {
                r.setProperty("pFirstID", et_edge_id);
                System.out.println("  add property pFirstID "+et_edge_id);
            } else {
                r.setProperty("pSecondID", et_edge_id);
                System.out.println("  add property pSecondID "+et_edge_id);

            }
            tx.success();
        }

        if (needtoCloseDB) {
            neo4j.closeDB();
        }
    }

    /**
     * Get the spanning tree of the graph.
     * Get number of components
     * Get the relationships that consists of the spanning tree
     */
    public void KruskalMST() {
        SpTree = new Relationship[N - 1];
        int e = 0;
        int i = 0;
        while (e < N - 1) {
            Relationship rel = rels[i++];
            int src_id = (int) rel.getStartNodeId();
            int dest_id = (int) rel.getEndNodeId();

            int src_root = find(src_id);
            int dest_root = find(dest_id);
//            System.out.println(src_id + " " + src_root + "  " + dest_id + "  " + dest_root + " " + rel);
            if (src_root != dest_root) {
                SpTree[e++] = rel;
                union(src_root, dest_root);
                connect_component_number--;
            }
        }

//        System.out.println(connect_component_number);
//        for (UFnode a : unionfind) {
//            System.out.println(a);
//        }
//
//        for (Relationship r : this.SpTree) {
//            System.out.println(r);
//        }

        System.out.println("================Finish the First Round Spanning Tree Finding==================");
    }

    /**
     * Union the lower rank component to the higher rank component
     * If the rank of two components are same, union the dest component to the src component.
     * Then increase the rank ot the src component.
     *
     * @param src_root  the root of src node in UF structure
     * @param dest_root the root of dest node in UF structure
     */
    private void union(int src_root, int dest_root) {
        if (unionfind[src_root].rank < unionfind[dest_root].rank) {
            unionfind[src_root].parentID = dest_root;
            unionfind[dest_root].size += unionfind[src_root].size;
        } else if (unionfind[src_root].rank > unionfind[dest_root].rank) {
            unionfind[dest_root].parentID = src_root;
            unionfind[src_root].size += unionfind[dest_root].size;
        } else {
            unionfind[dest_root].parentID = src_root;
            unionfind[src_root].rank++;
            unionfind[src_root].size += unionfind[dest_root].size;
        }
    }

    /**
     * @param src_id the node id
     * @return the root id of src_id
     */
    private int find(int src_id) {
        while (unionfind[src_id].parentID != src_id) {
            src_id = unionfind[src_id].parentID;
        }
        return src_id;
    }

}

class UFnode {
    int parentID;
    int nodeID;
    int rank;
    int size; //size of the subtree, include current node;

    public UFnode(int nodeID) {
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
