package test;

import Neo4jTools.CreateDB;
import Neo4jTools.Neo4jDB;
import org.neo4j.graphdb.*;
import v3.DynamicForests;
import v3.SpanningForests;
import v3.SpanningTree;

import java.util.Map;

public class testDynamicForests {
    DynamicForests dforests;

    public testDynamicForests() {
        this.dforests = new DynamicForests();
    }

    public static void main(String args[]) {
        testDynamicForests t = new testDynamicForests();
        t.test(16, 0.0);
    }

    private void test(int graph_size, double samnode_t) {
        createdb(graph_size, samnode_t);
        String sub_db_name = graph_size + "-" + samnode_t + "-" + "Level" + 0;
        Neo4jDB neo4j = new Neo4jDB(sub_db_name);
        System.out.println(neo4j.DB_PATH);
        neo4j.startDB(false);
        SpanningTree sptree = new SpanningTree(neo4j, true);


        customizedTheSpanningTreeEdge(sptree, neo4j);

        System.out.println("number of tree edges : " + sptree.SpTree.size() + "   number of tree nodes : " + sptree.N_nodes.size());

//        for(Relationship r:sptree.SpTree){
//            System.out.println(r);
//        }


        sptree.FindAdjList();
        System.out.println("~~~~~~~~~~~~~~~~~~~");
        sptree.FindEulerTourString(0);
        this.dforests.createBase(sptree);
//        this.dforests.dforests.get(0).trees.get(0).rbtree.root.print();
//        try(Transaction tx = neo4j.graphDB.beginTx()){
//            System.out.println(neo4j.graphDB.getRelationshipById(13));
//            for(Map.Entry<String, Object> ssss:neo4j.graphDB.getRelationshipById(13).getAllProperties().entrySet()){
//                System.out.println(ssss.getKey()+"    "+ ssss.getValue());
//            }
//            tx.success();
//        }
//
        deleteTest(7, neo4j);
        deleteTest(13, neo4j);
//
        deleteTest(8, neo4j);
        deleteTest(1, neo4j);


        for(Map.Entry<Integer, SpanningForests> sf : dforests.dforests.entrySet()){
            for(SpanningTree sp_tree:sf.getValue().trees){
                sp_tree.rbtree.root.print();
                sp_tree.printNodes();
                sp_tree.printEdges();
                System.out.println("---------------------------------");
            }
            System.out.println("===================================================");
        }
//
//        try(Transaction tx = neo4j.graphDB.beginTx()){
//            ResourceIterable<Relationship> a = neo4j.graphDB.getAllRelationships();
//            ResourceIterator<Relationship> b = a.iterator();
//            while(b.hasNext()){
//                Relationship r = b.next();
//                System.out.println(r+"   "+r.getProperty("level"));
//                for(Map.Entry<String, Object> pp :r.getAllProperties().entrySet()){
//                    System.out.println("  "+pp.getKey()+" <---->  "+pp.getValue());
//
//                }
//            }
//            tx.success();
//        }


//        this.dforests.dforests.get(1).trees.get(0).rbtree.root.print();


        neo4j.closeDB();
    }

    //Todo:Change the logic: if it is could be deleted, then remove it from highest level to lowest level. Not deleted it once found a replacement edge.
    private void deleteTest(int rel_id, Neo4jDB neo4j) {
        GraphDatabaseService graphdb = neo4j.graphDB;
        try (Transaction tx = graphdb.beginTx()) {
            Relationship r = graphdb.getRelationshipById(rel_id);
            System.out.println("Start to delete the relationship "+r);

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
            }
            tx.success();
        }
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

    private void createdb(int graph_size, double samnode_t) {
        CreateDB c = new CreateDB();
        c.createBusLineDataBase(graph_size, samnode_t);
    }

    private void customizedTheSpanningTreeEdge(SpanningTree sptree, Neo4jDB neo4j) {
        addSpanningTreeEdge(0, 1, sptree, neo4j);
        addSpanningTreeEdge(1, 2, sptree, neo4j);
        addSpanningTreeEdge(2, 3, sptree, neo4j);
        addSpanningTreeEdge(3, 4, sptree, neo4j);
        addSpanningTreeEdge(4, 5, sptree, neo4j);
        addSpanningTreeEdge(7, 2, sptree, neo4j);
        addSpanningTreeEdge(8, 12, sptree, neo4j);
        addSpanningTreeEdge(9, 4, sptree, neo4j);
        addSpanningTreeEdge(9, 10, sptree, neo4j);
        addSpanningTreeEdge(11, 10, sptree, neo4j);
        addSpanningTreeEdge(6, 11, sptree, neo4j);
        addSpanningTreeEdge(12, 13, sptree, neo4j);
        addSpanningTreeEdge(13, 14, sptree, neo4j);
        addSpanningTreeEdge(14, 15, sptree, neo4j);
        addSpanningTreeEdge(15, 11, sptree, neo4j);
        addSpanningTreeEdge(16, 15, sptree, neo4j);
    }

    private void addSpanningTreeEdge(long sid, int eid, SpanningTree sptree, Neo4jDB neo4j) {
        GraphDatabaseService graphdb = neo4j.graphDB;
        try (Transaction tx = graphdb.beginTx()) {
            ResourceIterable<Relationship> r_iterable = graphdb.getAllRelationships();
            ResourceIterator<Relationship> r_iter = r_iterable.iterator();
            while (r_iter.hasNext()) {
                Relationship r = r_iter.next();
                if ((sid == r.getStartNodeId() && eid == r.getEndNodeId()) || (sid == r.getEndNodeId() && eid == r.getStartNodeId())) {
                    sptree.SpTree.add(r.getId());
                    sptree.updateNodesIDInformation(r);
                    System.out.println(r);
                }
            }
            tx.success();
        }
    }
}
