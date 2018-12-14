package v3;

import Neo4jTools.Neo4jDB;
import configurations.ProgramProperty;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;

public class SpanningTree {
    public ProgramProperty prop = new ProgramProperty();
    int E = 0; // number of edges
    int N = 0; // number of nodes
    String DBPath;

    long graphsize;
    double samenode_t;
    int connect_component_number = 0; // number of the connect component found in the graph
    Relationship[] rels;
    UFnode unionfind[];
    Relationship SpTree[];
    Bag adjList[];


    public SpanningTree(int graphsize, int samenode_t) {
        this.graphsize = graphsize;
        this.samenode_t = samenode_t;
        initiliztion();
    }


    public static void main(String args[]) {
        SpanningTree spt = new SpanningTree(14, 0);
        spt.EulerTourString();
    }

    private void EulerTourString() {
        KruskalMST();
        FindAdjList();
        FindEulerTourString(0);
    }

    private void FindEulerTourString(int src_id) {
        Boolean[] visitied_ary = new Boolean[N]; //default all the nodes are un-visited
        for (int i = 0; i < visitied_ary.length; i++) {
            visitied_ary[i] = false;
        }

        RelationshipExt iter_edge = adjList[src_id].getFirstUnvisitedOutgoingEdge();
        int current_start_id = -1, current_end_id = -1;
        if (iter_edge != null) {
            current_start_id = iter_edge.start_id;
            current_end_id = iter_edge.end_id;
        }

        //Todo: Termination Condition
        //1) All the outgoing edges are visited.
        //2) The current_end_id is the src node
        while (adjList[src_id].getFirstUnvisitedOutgoingEdge() != null || current_end_id != src_id) {
            System.out.println(iter_edge.relationship + "  " + current_start_id + " " + current_end_id);
            iter_edge.visited = true;
            iter_edge = adjList[current_end_id].getFirstUnvisitedOutgoingEdge();
            System.out.println(iter_edge.relationship + "  " + iter_edge.start_id + " " + iter_edge.end_id);
            if (iter_edge != null) {
                current_start_id = iter_edge.start_id;
                current_end_id = iter_edge.end_id;
            }
            System.out.println((adjList[src_id].getFirstUnvisitedOutgoingEdge() == null) + "   " + (current_end_id == src_id));
            System.out.println("--------------------------------------------------------");
        }

    }

    private void FindAdjList() {
        adjList = new Bag[N];

        for (int i = 0; i < N; i++) {
            adjList[i] = new Bag();
        }

        for (Relationship r : SpTree) {
            int src_id = (int) r.getStartNodeId();
            int dest_id = (int) r.getEndNodeId();
            RelationshipExt rel_ext = new RelationshipExt(r, src_id, dest_id);
            adjList[src_id].add(rel_ext);
            RelationshipExt rel_ext_reverse = new RelationshipExt(r, dest_id, src_id);
            adjList[dest_id].add(rel_ext_reverse);
        }
    }

    private void initiliztion() {
        DBPath = prop.params.get("neo4jdb");
        String sub_db_name = graphsize + "-" + samenode_t + "-Level" + 0;
        System.out.println(sub_db_name);
        Neo4jDB neo4j = new Neo4jDB(sub_db_name);
        neo4j.startDB();
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
                rels[i++] = r;
            }
            tx.success();
        }


//        for (i = 0; i < rels.length; i++) {
//            System.out.println(i + "  " + rels[i].getStartNodeId() + "  ----->  " + rels[i].getEndNodeId());
//        }


        neo4j.closeDB();
    }

    void KruskalMST() {
        SpTree = new Relationship[N - 1];
        int e = 0;
        int i = 0;
        while (e < N - 1) {
            Relationship rel = rels[i++];
            int src_id = (int) rel.getStartNodeId();
            int dest_id = (int) rel.getEndNodeId();

            int src_root = find(src_id);
            int dest_root = find(dest_id);
            System.out.println(src_id + " " + src_root + "  " + dest_id + "  " + dest_root + " " + rel);

            if (src_root != dest_root) {
                SpTree[e++] = rel;
                union(src_root, dest_root);
                connect_component_number--;
            }
        }

        System.out.println(connect_component_number);
        for (UFnode a : unionfind) {
            System.out.println(a);
        }

        for (Relationship r : this.SpTree) {
            System.out.println(r);
        }

        System.out.println("================Finish the First Round Spanning Tree Finding==================");
    }

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
