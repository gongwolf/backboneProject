package Baseline;

import DataStructure.Monitor;
import javafx.util.Pair;
import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class BBSBuslineTest {

    ArrayList<Pair<Long, Long>> queries = new ArrayList<>();

    public static void main(String args[]) {
        BBSBuslineTest bbstest = new BBSBuslineTest();

        bbstest.queries.add(new Pair<>(9700L, 3669L));
        bbstest.queries.add(new Pair<>(9519L, 4502L));
        bbstest.queries.add(new Pair<>(688L, 4280L));
        bbstest.queries.add(new Pair<>(4348L, 4087L));
        bbstest.queries.add(new Pair<>(4816L, 3131L));

//        bbstest.generateQueries(5);

        bbstest.test(true, true);
        System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
        bbstest.test(true, false);
//        System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
//        bbstest.test(false, true);
//        System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
//        bbstest.test(false, false);
    }

    private void generateQueries(int numOfQueries) {
        this.queries.clear();
        BBSBaselineBusline baseline = new BBSBaselineBusline();

        ArrayList<Long> nodelist = new ArrayList<>();

        try (Transaction tx = baseline.neo4j.graphDB.beginTx()) {
            ResourceIterable<Node> nodes_iterable = baseline.neo4j.graphDB.getAllNodes();
            ResourceIterator<Node> nodes_iter = nodes_iterable.iterator();
            while (nodes_iter.hasNext()) {
                long node_id = nodes_iter.next().getId();
                nodelist.add(node_id);
            }
            tx.success();
        }

        for(int i = 0 ; i < numOfQueries ;i++){
            long src = getRondomNodes(nodelist);
            long dest = getRondomNodes(nodelist);
            this.queries.add(new Pair<>(src, dest));
        }
        baseline.closeDB();

    }

    private void test(boolean init, boolean uselandmark) {
        Monitor monitor = new Monitor();

//        BBSBaselineBusline baseline = new BBSBaselineBusline(10000, 2.844, 6);
        BBSBaselineBusline baseline = new BBSBaselineBusline();
        System.out.println("number of nodes " + baseline.neo4j.getNumberofNodes());
        System.out.println("number of edges " + baseline.neo4j.getNumberofEdges());
        if (uselandmark) {
            baseline.buildLandmarkIndex(3);

//            for (Map.Entry<Long, HashMap<Long, double[]>> landindex :
//                    baseline.landmark_index.entrySet()) {
//
//                long sid = landindex.getKey();
//                for (Map.Entry<Long, double[]> did_index : landindex.getValue().entrySet()) {
//                    long did = did_index.getKey();
//                    double costs[] = did_index.getValue();
//                    // "EDistence","MetersDistance","RunningTime"
//                    double c1 = baseline.myDijkstra(sid, did, "EDistence");
//                    double c2 = baseline.myDijkstra(sid, did, "MetersDistance");
//                    double c3 = baseline.myDijkstra(sid, did, "RunningTime");
//
//                    boolean equal = (costs[0] == c1 && costs[1] == c2 && costs[2] == c3);
//                    if (!equal) {
//                        System.out.println(sid + "   ---->>>>  " + did + " " + c1 + " " + c2 + " " + c3 + " " + equal);
//                    }
//                }
//            }
        }

        long start_ms = System.currentTimeMillis();


        for (int i = 0; i < queries.size(); i++) {
            int sizeofinit = 0;

            long src = this.queries.get(i).getKey();
            long dest = this.queries.get(i).getValue();

            System.out.println(src + " =============> " + dest);

            if (init) {
                baseline.initilizeSkylinePath(src, dest);
                sizeofinit = baseline.results.size();
//                for (path c : baseline.results) {
//                    System.out.println(c);
//                }
                baseline.monitor.spInitTimeInBaseline = (System.currentTimeMillis() - start_ms);
            }


//            System.out.println("=======================================");

            baseline.queryOnline(src, dest);
            long end_ms = System.currentTimeMillis();
            baseline.monitor.overallRuningtime = end_ms - start_ms;
//            System.out.println("=======================================");
            monitor.clone(baseline.monitor);

            ArrayList<path> results = new ArrayList<>();
            results.addAll(baseline.results);
//            System.out.println("# of node add to skyline function : " + baseline.monitor.node_call_addtoskyline + "    # of add to skyline in final:" + baseline.monitor.callAddToSkyline);
//            System.out.println("# dominated checking :" + baseline.monitor.callcheckdominatedbyresult);
//            System.out.println("# of coverd node during the query process： " + baseline.monitor.coveredNodes);
//            System.out.println(baseline.monitor.getRunningtime_check_domination_resultByms());
//            System.out.println(baseline.monitor.allsizeofthecheckdominatedbyresult);
            System.out.println("results size " + results.size());
            System.out.println(baseline.monitor.getOverallRuningtime_in_Sec() + " s");
            System.out.println("=======================================================================");
//            for (path c : baseline.results) {
//                System.out.println(c);
//            }
//            System.out.println("=======================================================================");
            System.out.println("=======================================================================");
            baseline.clear();
        }
        baseline.closeDB();

    }

    private long getRondomNodes(ArrayList<Long> nodelist) {
        Random r = new Random();
        int idx = r.nextInt(nodelist.size());
        return nodelist.get(idx);
    }


}