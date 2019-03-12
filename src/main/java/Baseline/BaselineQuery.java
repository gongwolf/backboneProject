package Baseline;

import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class BaselineQuery {

    private int graphsize;

    public BaselineQuery(long graphsize) {
        this.graphsize = Math.toIntExact(graphsize);
    }

    public static void main(String arg[]) {

        long graphsize = 40000;

        BaselineQuery bq = new BaselineQuery(graphsize);
//        bq.batchRandomQueries();
        bq.query(7732, 167, false);
        bq.query(7732, 167, true);
    }

    public void query(long src, long dest, boolean init) {
        System.out.println(src+"   >>>>>>    "+dest);
        ArrayList<path> skylines = onlineQueryTest(src, dest, init);
        System.out.println();
        for (path p : skylines) {
            System.out.println(p);
        }

    }

    public void batchRandomQueries() {
        for (int i = 0; i < 10; i++) {
            long startNode = getRandomNumberInRange(graphsize);
            long endNode = getRandomNumberInRange(graphsize);

            if (startNode != endNode) {
                System.out.print(startNode + " >>>>  " + endNode + "  ");
                onlineQueryTest(startNode, endNode, true);
                System.out.print("          ");
                onlineQueryTest(startNode, endNode, false);
                System.out.print("\n");
            }
        }
    }


    public ArrayList<path> onlineQueryTest(long startNode, long endNode, boolean init) {
        BBSBaseline baseline = new BBSBaseline(this.graphsize, 4, 3);
        baseline.results.clear();

        long start_ms = System.currentTimeMillis();

        int sizeofinit = 0;
        if (init) {
            baseline.initilizeSkylinePath(startNode, endNode);
            sizeofinit = baseline.results.size();
        }

        baseline.queryOnline(startNode, endNode);
        long end_ms = System.currentTimeMillis();
        System.out.println("running time: " + (end_ms - start_ms) + " ms   size of the result: " + (init ? sizeofinit + " --> " : "") + baseline.results.size());
        baseline.closeDB();


        ArrayList<path> results = new ArrayList<>();
        results.addAll(baseline.results);
        System.out.println(baseline.nodes_call_addtoSkylineFunction+"    "+baseline.callAddToSkylineFunction);
        return results;
    }

    public long getRandomNumberInRange(long graphsize) {

        if (0 >= graphsize) {
            throw new IllegalArgumentException("the size of the graph must greater than 0");
        }

        return ThreadLocalRandom.current().nextLong(graphsize);
    }

}
