package Baseline;

import DataStructure.Monitor;
import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.Random;

public class BBSBuslineTest {
    public static void main(String args[]) {
        BBSBuslineTest bbstest = new BBSBuslineTest();
        for (int i = 0; i < 1; i++) {
            bbstest.test(true);
        }
    }

    private void test(boolean init) {
        Monitor monitor = new Monitor();
        long start_ms = System.currentTimeMillis();

        BBSBaselineBusline baseline = new BBSBaselineBusline(10000, 2.844, 6);
        System.out.println("number of nodes "+ baseline.neo4j.getNumberofNodes());
        System.out.println("number of edges "+baseline.neo4j.getNumberofEdges());
        baseline.buildLandmarkIndex(3);

//        ArrayList<Long> nodelist = new ArrayList<>();
//
//        try (Transaction tx = baseline.neo4j.graphDB.beginTx()) {
//            ResourceIterable<Node> nodes_iterable = baseline.neo4j.graphDB.getAllNodes();
//            ResourceIterator<Node> nodes_iter = nodes_iterable.iterator();
//            while (nodes_iter.hasNext()) {
//                long node_id = nodes_iter.next().getId();
//                nodelist.add(node_id);
//            }
//            tx.success();
//        }
//
//        int sizeofinit = 0;
//
//        long src = getRondomNodes(nodelist);
//        long dest = getRondomNodes(nodelist);
//        System.out.println(src + " =============> "+dest);
//
//        if (init) {
//            baseline.initilizeSkylinePath(src, dest);
//            sizeofinit = baseline.results.size();
//            baseline.monitor.spInitTimeInBaseline = (System.currentTimeMillis() - start_ms);
//        }
//
//        System.out.println("=======================================");
//
//        baseline.queryOnline(src, dest);
//        long end_ms = System.currentTimeMillis();
//        baseline.monitor.overallRuningtime = end_ms - start_ms;
//        baseline.closeDB();
//        System.out.println("=======================================");
//
//        monitor.clone(baseline.monitor);
//
//        ArrayList<path> results = new ArrayList<>();
//        results.addAll(baseline.results);
//        System.out.println("# of node add to skyline function : " + baseline.monitor.node_call_addtoskyline + "    # of add to skyline in final:" + baseline.monitor.callAddToSkyline);
//        System.out.println("# dominated checking :" + baseline.monitor.callcheckdominatedbyresult);
//        System.out.println("# of coverd node during the query process： " + baseline.monitor.coveredNodes);
//        System.out.println(baseline.monitor.getRunningtime_check_domination_resultByms());
//        System.out.println(baseline.monitor.allsizeofthecheckdominatedbyresult);
//        System.out.println(baseline.monitor.getOverallRuningtime_in_Sec());
//        System.out.println("=======================================================================");
//        System.out.println("=======================================================================");
//        System.out.println("=======================================================================");
    }

    private long getRondomNodes(ArrayList<Long> nodelist) {
        Random r = new Random();
        int idx = r.nextInt(nodelist.size());
        return nodelist.get(idx);
    }


}
