package Baseline;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;

public class BBSBuslineTest {
    public static void main(String args[]) {
        BBSBuslineTest bbstest = new BBSBuslineTest();
        bbstest.test(60);
    }

    private void test(long nodeid) {
        BBSBaselineBusline baseline = new BBSBaselineBusline(10000, 2.844, 7);
//        baseline.bbs(60);

        try (Transaction tx = baseline.neo4j.graphDB.beginTx()) {
            ResourceIterable<Node> allnodes_iteratable = baseline.neo4j.graphDB.getAllNodes();
            ResourceIterator<Node> allnodes_iter = allnodes_iteratable.iterator();

            int i = 1;
            while (allnodes_iter.hasNext()) {
                Node dest_node = allnodes_iter.next();
                if (nodeid != dest_node.getId()) {
                    ArrayList<path> results = baseline.queryOnline(nodeid, dest_node.getId());
                    System.out.println((i++)+" : "+nodeid+" --->   "+dest_node.getId()+"  "+results.size());
                }
            }
        }

        baseline.neo4j.closeDB();
    }
}
