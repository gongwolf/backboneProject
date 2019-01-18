package test;

import Neo4jTools.Neo4jDB;
import org.neo4j.graphdb.*;
import v3.SpanningTree;

public class testDynamicForests {
    public static void main(String args[]){
        testDynamicForests t = new testDynamicForests();
        t.test(16,0.0);
    }

    private void test(int graph_size, double samnode_t) {
        String sub_db_name = graph_size + "-" + samnode_t + "-" + "Level" + 0;
        Neo4jDB neo4j = new Neo4jDB(sub_db_name);
        System.out.println(neo4j.DB_PATH);
        neo4j.startDB();
        SpanningTree sptree = new SpanningTree(neo4j, true);

        GraphDatabaseService graphdb = neo4j.graphDB;
        try(Transaction tx = graphdb.beginTx()){
            ResourceIterable<Relationship> r_iterable = graphdb.getAllRelationships();
            ResourceIterator<Relationship> r_iter = r_iterable.iterator();

            while (r_iter.hasNext()){
                Relationship r = r_iter.next();
                System.out.println(r);
            }
            tx.success();
        }
//        sptree.SpTree.add()


        neo4j.closeDB();
    }
}
