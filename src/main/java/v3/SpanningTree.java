package v3;

import DataStructure.RedBlackTree;
import DataStructure.TNode;
import Neo4jTools.Neo4jDB;
import configurations.ProgramProperty;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import v3.Bag.Node;

import java.util.*;

import static DataStructure.STATIC.nil;

public class SpanningTree {
    public int level;
    public ProgramProperty prop = new ProgramProperty();
    public Neo4jDB neo4j = null;
    //Store the relationships that consists the spanning tree.
//    Relationship SpTree[];
    public HashSet<Relationship> SpTree;
    public RedBlackTree rbtree = new RedBlackTree();
    public HashSet<Long> N_nodes;
    int E = 0; // number of edges
    int N = 0; // number of nodes
    String DBPath;
    long graphsize;
    double samenode_t;
    int connect_component_number = 0; // number of the connect component found in the graph
    Relationship[] rels;
    UFnode unionfind[];
    //the adjacent list of the spanning tree
    Bag adjList[];
    boolean isSingle = false;

    int insertedEdgesTimes = 0;


    public SpanningTree() {
        SpTree = new HashSet<>();
        N_nodes = new HashSet<>();
    }

    public SpanningTree(int graphsize, double samenode_t) {
        this.graphsize = graphsize;
        this.samenode_t = samenode_t;
        this.level = 0;
        initialization();
    }


    public SpanningTree(int graphsize, double samenode_t, int current_level) {
        this.graphsize = graphsize;
        this.samenode_t = samenode_t;
        this.level = current_level;
        initialization();
    }

    /**
     * @param neo4j
     * @param init  if it is true, call the initialization() function that gets the information by reading the graph database from disk;
     */
    public SpanningTree(Neo4jDB neo4j, boolean init) {
        this.neo4j = neo4j;
        SpTree = new HashSet<>();
        N_nodes = new HashSet<>();

        if (init) {
            initialization();
        }
    }


    public static void main(String args[]) {
        SpanningTree spt = new SpanningTree(14, 0);
        spt.EulerTourString();
    }

    public String EulerTourString(TreeMap<Long, GraphNode> graph_node_spanning_rb_map, int level) {
        KruskalMST();
        FindAdjList();
        //search the Euler Tour of the spanning tree from node 0
        String eulertourString = FindEulerTourString(0, graph_node_spanning_rb_map, level);
        return eulertourString;
    }


    public String EulerTourString() {
        KruskalMST();
        FindAdjList();
        String elurtourString = FindEulerTourString(0);
        return elurtourString;
    }

    public String EulerTourString(int level) {
        KruskalMST();
        FindAdjList();
        String elurtourString = FindEulerTourString(0, level);
        return elurtourString;
    }


    private String FindEulerTourString(int src_id, TreeMap<Long, GraphNode> graph_node_spanning_rb_map, int level) {
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
            updateRelationshipRBPointer(iter_edge, et_edge_id, -1, level);
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

                updateRelationshipRBPointer(iter_edge, et_edge_id, -1, level); //not org key, set org to -1
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


    public String FindEulerTourString(int src_id) {
        int et_edge_id = 0;

        RelationshipExt iter_edge = adjList[src_id].getFirstUnvisitedOutgoingEdge();
        int current_start_id = -1, current_end_id = -1;


        if (iter_edge != null) {
            current_start_id = iter_edge.start_id;
            current_end_id = iter_edge.end_id;
            TNode<RelationshipExt> node = new TNode<RelationshipExt>(et_edge_id, iter_edge);
            updateRelationshipRBPointer(iter_edge, et_edge_id, -1, 0);
            rbtree.insert(node);
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
//            System.out.println("next edge: " + iter_edge.relationship + "  " + iter_edge.start_id + " " + iter_edge.end_id);
            if (iter_edge != null) {
                current_start_id = iter_edge.start_id;
                current_end_id = iter_edge.end_id;
                TNode<RelationshipExt> node = new TNode<>(et_edge_id, iter_edge);
                updateRelationshipRBPointer(iter_edge, et_edge_id, -1, 0);
                rbtree.insert(node);
                et_edge_id++;
            }
//            System.out.println((adjList[src_id].getFirstUnvisitedOutgoingEdge() == null) + "   " + (current_end_id == src_id));
            sb.append("[").append(current_start_id).append(",").append(current_end_id).append("]-");
            sb1.append(current_start_id).append(",");
//            System.out.println("--------------------------------------------------------");
        }

//        System.out.println(sb.toString().substring(0, sb.length() - 1));
        System.out.println(sb1.append(src_id));
        this.insertedEdgesTimes = N * 2;
        System.out.println("inserted edges times : " + insertedEdgesTimes);
        return sb.toString().substring(0, sb.length() - 1);
    }


    public String FindEulerTourString(int src_id, int level) {
        int et_edge_id = 0;
        RelationshipExt iter_edge = adjList[src_id].getFirstUnvisitedOutgoingEdge();
        int current_start_id = -1, current_end_id = -1;

        if (iter_edge != null) {
            current_start_id = iter_edge.start_id;
            current_end_id = iter_edge.end_id;
            TNode<RelationshipExt> node = new TNode<>(et_edge_id, iter_edge);
            rbtree.insert(node);
            updateRelationshipRBPointer(iter_edge, et_edge_id, -1, level);
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
            iter_edge.visited = true;
            iter_edge = getProperUnvisitedOutgoingEdge(current_end_id);
            if (iter_edge != null) {
                current_start_id = iter_edge.start_id;
                current_end_id = iter_edge.end_id;
                TNode<RelationshipExt> node = new TNode<>(et_edge_id, iter_edge);
                rbtree.insert(node);
                updateRelationshipRBPointer(iter_edge, et_edge_id, -1, level);
                et_edge_id++;

            }
            sb.append("[").append(current_start_id).append(",").append(current_end_id).append("]-");
            sb1.append(current_start_id).append(",");
        }

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

    public void FindAdjList() {

        //because the nodes id starts from 0, the number of node is 1 greater than the max id

        int max_n = (int) (Collections.max(N_nodes) + 1);

        adjList = new Bag[max_n];

        for (int i = 0; i < adjList.length; i++) {
            if (N_nodes.contains((long) i)) {
                adjList[i] = new Bag();
//                System.out.println(i);
            }
        }

        System.out.println("==========================");
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
        System.out.println("==========================");
    }

    /**
     * Initialization:
     * Get the number of edges
     * Get the number of nodes
     * The init number of connect components is the number of the nodes
     * each node whose root is the node id
     */
    private void initialization() {
        boolean needtoCloseDB = false;
        this.N_nodes = new HashSet<>();

        if (neo4j == null) {
            DBPath = prop.params.get("neo4jdb");
            String sub_db_name = graphsize + "-" + samenode_t + "-Level" + level;
            neo4j = new Neo4jDB(sub_db_name);
            System.out.println("connected to db " + neo4j.DB_PATH);
            neo4j.startDB();
            needtoCloseDB = true;
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


    public void updateRelationshipRBPointer(RelationshipExt iter_edge, int et_edge_id, int org_key, int level) {
        boolean needtoCloseDB = false;
        if (neo4j == null) {
            DBPath = prop.params.get("neo4jdb");
            String sub_db_name = graphsize + "-" + samenode_t + "-Level" + level;
            neo4j = new Neo4jDB(sub_db_name);
            neo4j.startDB();
            needtoCloseDB = true;
            System.out.println("connected to db " + neo4j.DB_PATH);
        }

        try (Transaction tx = neo4j.graphDB.beginTx()) {
            Relationship r = iter_edge.relationship;
            if (!r.hasProperty("pFirstID" + level)) {
                r.setProperty("pFirstID" + level, et_edge_id);
            } else if (!r.hasProperty("pSecondID" + level)) {
                r.setProperty("pSecondID" + level, et_edge_id);
            } else {
                int firstID = (int) r.getProperty("pFirstID" + level);
                int secondID = (int) r.getProperty("pSecondID" + level);

                if (firstID == org_key) {
                    r.setProperty("pFirstID" + level, et_edge_id);
                } else if (secondID == org_key) {
                    r.setProperty("pSecondID" + level, et_edge_id);
                }

            }
            tx.success();
        }

        if (needtoCloseDB) {
            neo4j.closeDB();
        }
    }


    public void clearRelationshipRBPointerAtLevel(int level_r) {
        try (Transaction tx = this.neo4j.graphDB.beginTx()) {
            for (Relationship rel : this.SpTree) {
                if (rel.hasProperty("pFirstID" + level_r)) rel.removeProperty("pFirstID" + level_r);
                if (rel.hasProperty("pSecondID" + level_r)) rel.removeProperty("pSecondID" + level_r);
            }

            tx.success();
        }
    }

    /**
     * Get the spanning tree of the graph.
     * Get number of components
     * Get the relationships that consists of the spanning tree
     */
    public void KruskalMST() {
        SpTree = new HashSet<>();
        int e = 0;
        int i = 0;
        while (e < N - 1) {
            Relationship rel = rels[i++];
            int src_id = (int) rel.getStartNodeId();
            int dest_id = (int) rel.getEndNodeId();
            int src_root = find(src_id);
            int dest_root = find(dest_id);
            if (src_root != dest_root) {
                SpTree.add(rel);
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


    public boolean hasEdge(Relationship r) {
        TNode<RelationshipExt> n = this.rbtree.root;
        if (n == nil) {
            System.out.println("the root is empty ------ " + this.getClass() + " function: hasEdge    parameters--> r:" + r);
            return false;
        }
        Stack<TNode<RelationshipExt>> s = new Stack<>();
        while (n != nil || s.size() > 0) {
            while (n != null) {
                s.push(n);
                n = n.left;
            }

            n = s.pop();
            if (n != nil && n.item.relationship != null && n.item.relationship.getId() == r.getId()) {
                return true;
            }
            n = n.right;
        }

        return false;
    }

    public TNode<RelationshipExt> findMinimum() {
        return rbtree.findMinimum(rbtree.root);
    }

    public int findMaximumKeyValue() {
        return rbtree.findMaximumKeyValue(rbtree.root);
    }

    public TNode<RelationshipExt> findLeftSubTree(TNode<RelationshipExt> min_node, Relationship r, SpanningTree left_sub_tree) {

        //if the minimum node is the node needs to be deleted, left_sub_tree is empty, the splitor is the minimum node.
//        if (min_node.item.relationship.getId() == r.getId()) {
//            left_sub_tree.isSingle = true;
//            return min_node;
//        }
        TNode<RelationshipExt> node = new TNode<>(min_node);

        if (min_node.item.relationship.getId() == r.getId()) {
            return min_node;
        }

        left_sub_tree.insert(node);

        TNode<RelationshipExt> suc_node = rbtree.successor(min_node);
        while (suc_node.item.relationship.getId() != r.getId()) {
            node = new TNode<>(suc_node);
            left_sub_tree.insert(node);
            suc_node = rbtree.successor(suc_node);
        }
        return suc_node;
    }

    public TNode<RelationshipExt> findMiddleSubTree(TNode<RelationshipExt> splitor, Relationship r, SpanningTree middle_sub_tree) {

        TNode<RelationshipExt> node;

        TNode<RelationshipExt> suc_node = rbtree.successor(splitor);

        //if the successor of the splitor is the second appear of the node needs to be deleted,
        // middle_sub_tree is empty, the splitor is the successor of the splitor.
        // the while loop would not run
        while (suc_node.item.relationship.getId() != r.getId()) {
            node = new TNode<>(suc_node);
            middle_sub_tree.insert(node);
            suc_node = rbtree.successor(suc_node);
        }
        return suc_node;
    }

    public void findRightSubTree(TNode<RelationshipExt> Splitor, SpanningTree last_sub_tree) {
        TNode<RelationshipExt> suc_node = rbtree.successor(Splitor);

        //If there is not successor follow the given Splitor, the last_sub_tree is a empty tree
        while (suc_node != nil) {
            TNode<RelationshipExt> node = new TNode<>(suc_node);
            last_sub_tree.insert(node);

//            System.out.println(suc_node.item);
            suc_node = rbtree.successor(suc_node);
        }

    }


    public void insert(TNode<RelationshipExt> node) {
        this.rbtree.insert(node);
        updateNodesIDInformation(node.item.relationship);
        if (!this.SpTree.contains(node.item.relationship)) {
            this.SpTree.add(node.item.relationship);
        }
        this.N = N_nodes.size();
        this.E = SpTree.size();
        this.insertedEdgesTimes++;
    }

    public void fixIfSingle(Relationship r) {
        if (this.N == 2 && this.insertedEdgesTimes == 1) {
            isSingle = true;
            System.out.print("Fix 2,1 single tree");
            TNode<RelationshipExt> f = null;

            if (rbtree.root != nil) {
                f = rbtree.root;
            }

            System.out.println(" :  " + f.item);

            RelationshipExt ext_r = new RelationshipExt(null, f.item.end_id, f.item.end_id);
            TNode<RelationshipExt> dummyRoot = new TNode<>(0, ext_r);


            this.rbtree.root = nil; //empty the tree
            this.insert(dummyRoot); //insert the dummy node as the new root

            //update information of this spanning tree
            this.N_nodes.clear();
            this.N_nodes.add((long) f.item.end_id);
            this.SpTree.clear();

            this.N = 1;
            this.E = 0;
        } else if (this.N == 2 && this.insertedEdgesTimes == 2) {

            TNode<RelationshipExt> f;

            if (rbtree.root.left != nil) {
                f = rbtree.root.left;
            } else {
                f = rbtree.root;
            }

            if (r.getId() == f.item.relationship.getId()) {
                isSingle = true;
                System.out.println("Fix 2,2 single tree : ");
                RelationshipExt ext_r = new RelationshipExt(null, f.item.end_id, f.item.end_id);
                TNode<RelationshipExt> dummyRoot = new TNode<>(0, ext_r);
                this.rbtree.root = nil; //empty the tree
                this.insert(dummyRoot); //insert the dummy node as the new root

                //update information of this spanning tree
                this.N_nodes.clear();
                this.N_nodes.add((long) f.item.end_id);
                this.SpTree.clear();

                this.N = 1;
                this.E = 0;
            }
        }
    }

    public void combineTree(SpanningTree right_sub_tree) {
        if (!right_sub_tree.isSingle) {
            TNode<RelationshipExt> right_min = right_sub_tree.findMinimum();
            TNode<RelationshipExt> node = new TNode<>(right_min);
            this.insert(node);

            TNode<RelationshipExt> suc_node = rbtree.successor(right_min);
            while (suc_node != nil) {
                node = new TNode<>(suc_node);
                this.insert(node);
                suc_node = rbtree.successor(suc_node);
            }
        }
    }

    /**
     * Update the tree edge level whose level is equal to i by increasing one
     *
     * @param i the given level
     */
    public void updateTreeEdgeLevel(int i) {
        for (Relationship rel : SpTree) {
            int level = (int) rel.getProperty("level");
            if (i == level) {
                int newlevel = level + 1;
                rel.setProperty("level", newlevel);
                System.out.println("update tree edge relationship " + rel + " at level " + i + " to " + newlevel);
            }
        }
    }

    public Relationship findReplacementEdge(SpanningTree another_sub_tree, int level, Relationship deletedRel) {
        System.out.println("Finding the replacement edge.................................");

        for (long nid : this.N_nodes) {
            Iterable<Relationship> iterable = neo4j.graphDB.getNodeById(nid).getRelationships(Direction.BOTH);
            Iterator<Relationship> iterator = iterable.iterator();
            while (iterator.hasNext()) {
                Relationship rel = iterator.next();
                int org_level = (int) rel.getProperty("level");


                /**
                 * rel is a relationship whose level is equal to specific level, and its end nodes is a tree node of this spanning tree.
                 * If rel connected this spanning tree to another_sub_tree, return it and connect those two subtree later
                 * else, increment the edge level by one
                 */

                if (org_level == level && rel.getId() != deletedRel.getId() && !this.SpTree.contains(rel)) {
                    if (another_sub_tree.N_nodes.contains(rel.getOtherNodeId(nid)) && rel.getId() != deletedRel.getId()) {
                        System.out.println(org_level + " <----->  " + level + "   " + rel);
                        return rel;
                    } else {
                        System.out.println("update relationship " + rel + " at level  to " + (level + 1));
                    }
                }
            }
        }
        return null;
    }

    public void reroot(long nid, HashMap<Integer, Integer> keyUpdatesMap, long eid, int level) {
        System.out.println("-------------------  reroot  --------------------------");
        this.rbtree.root.print();
//        System.out.println("0-0--0-0-0-0-0-0-0-0-0-0-0-0-0-0-0--00-0-0-0-0-0-0-0-0-0--");
        if (!this.isSingle) {
            TNode<RelationshipExt> min_node = findMinimum();
            System.out.println("min node in the tree " + min_node.item);

            int max_key = -1;
            if (level == 0) {
                max_key = findMaximumKeyValue();
                System.out.println("max Key Value : " + max_key);
            }

            TNode<RelationshipExt> node_temp = new TNode<>(min_node);
            rbtree.delete(node_temp);
            int org_key = node_temp.key;
            if (level == 0) {
                node_temp.key = ++max_key;
                int new_key = node_temp.key;
                keyUpdatesMap.put(org_key, new_key);
            } else {
                node_temp.key = keyUpdatesMap.get(org_key);                    //find the updated key for the node
            }
            rbtree.insert(node_temp);
            updateRelationshipRBPointer(node_temp.item, node_temp.key, org_key, level);
//            System.out.println(node_temp.item + "  " + node_temp.key);
            TNode<RelationshipExt> suc_node = rbtree.successor(min_node);
//            int k = 0;
            while (suc_node.item.start_id != nid) {
//                k++;
                node_temp = new TNode<>(suc_node);
                rbtree.delete(node_temp);
                org_key = node_temp.key;

                if (level == 0) {
                    //new node that is used to insert to the end of the tree
                    node_temp.key = ++max_key;
                    int new_key = node_temp.key;
                    keyUpdatesMap.put(org_key, new_key);
                } else {
                    node_temp.key = keyUpdatesMap.get(org_key);
                }
                rbtree.insert(node_temp);
                updateRelationshipRBPointer(node_temp.item, node_temp.key, org_key, level);
//                System.out.println(node_temp.item + "  " + node_temp.key);

//                System.out.println(suc_node.item);
                suc_node = rbtree.successor(suc_node);
//                if(k==13) break;
            }
            this.rbtree.root.print();
            System.out.println("=============================================");

        }
    }


    public void reroot(long nid, long eid, int level) {
        System.out.println("-------------------  reroot  --------------------------");
        this.rbtree.root.print();
        if (!this.isSingle) {
            TNode<RelationshipExt> min_node = findMinimum();
            System.out.println("min node in the tree " + min_node.item);

            int max_key = -1;
            if (level == 0) {
                max_key = findMaximumKeyValue();
                System.out.println("max Key Value : " + max_key);
            }

            TNode<RelationshipExt> node_temp = new TNode<>(min_node);
            rbtree.delete(node_temp);
            int org_key = node_temp.key;
            node_temp.key = ++max_key;
            rbtree.insert(node_temp);

            updateRelationshipRBPointer(node_temp.item, node_temp.key, org_key, level);
            TNode<RelationshipExt> suc_node = rbtree.successor(min_node);
            while (suc_node.item.start_id != nid) {

                node_temp = new TNode<>(suc_node);
                rbtree.delete(node_temp);
                org_key = node_temp.key;

                //new node that is used to insert to the end of the tree
                node_temp.key = ++max_key;
                rbtree.insert(node_temp);

                updateRelationshipRBPointer(node_temp.item, node_temp.key, org_key, level);

                suc_node = rbtree.successor(suc_node);
            }
            this.rbtree.root.print();
            System.out.println("=============================================");

        }
    }


    public void printNodes() {
        for (Long n : this.N_nodes) {
            System.out.print(n + " ");
        }
        System.out.println();
    }

    public void printEdges() {
        for (Relationship r : this.SpTree) {
            System.out.println(r + " ");
        }
    }


    public void deleteAdditionalInformationByRelationship(Relationship r) {
        this.SpTree.remove(r);
        long sid = r.getStartNodeId();
        long eid = r.getEndNodeId();

        if (SpTree.stream().filter(relationship -> relationship.getStartNodeId() == sid || relationship.getEndNodeId() == sid).count() == 0) {
            N_nodes.remove(sid);
        }
        if (SpTree.stream().filter(relationship -> relationship.getStartNodeId() == eid || relationship.getEndNodeId() == eid).count() == 0) {
            N_nodes.remove(eid);
        }
    }

    public boolean isEmpty() {
        return this.rbtree.root == nil;
    }

    public void initializedAsSingleTree(int nodeid) {
        RelationshipExt ext_r = new RelationshipExt(null, nodeid, nodeid);
        TNode<RelationshipExt> dummyRoot = new TNode<>(0, ext_r);
        this.rbtree.root = nil; //empty the tree
        this.insert(dummyRoot); //insert the dummy node as the new root
        this.N_nodes.clear();
        this.N_nodes.add((long) nodeid);
        this.SpTree.clear();
        this.N = 1;
        this.E = 0;
        this.isSingle = true;
    }

    public void clear() {
        this.N_nodes.clear();
        this.SpTree.clear();
        this.N = this.E = this.insertedEdgesTimes = 0;
        this.rbtree.root = nil;
    }

    public void copyTree(SpanningTree tree) {
        this.N_nodes = tree.N_nodes;
        this.N = tree.N;
        this.rbtree.root = tree.rbtree.root;
        this.SpTree = tree.SpTree;
        this.E = tree.E;

        this.isSingle = tree.isSingle;
        this.neo4j = tree.neo4j;
        this.adjList = tree.adjList;

        this.level = tree.level;
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
