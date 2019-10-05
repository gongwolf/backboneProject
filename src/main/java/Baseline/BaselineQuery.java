package Baseline;

import DataStructure.Monitor;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class BaselineQuery {

    private int dimension;
    private int graphsize;
    private int degree;

    public BaselineQuery(long graphsize) {
        this.graphsize = Math.toIntExact(graphsize);
        this.degree = 4;
    }

    public BaselineQuery(long graphsize, int degree) {
        this.graphsize = Math.toIntExact(graphsize);
        this.degree = degree;
    }

    public BaselineQuery(int graphsize, int degree, int dimension){
        this.degree = degree;
        this.graphsize = graphsize;
        this.dimension = dimension;
    }

    public static void main(String arg[]) {

        int graphsize = 2000;
        int degree = 4;
        int dimension = 3;

        BaselineQuery bq = new BaselineQuery(graphsize,degree,dimension);
        int src = bq.getRandomNumberInRange_int(0, graphsize - 1);
        int dest = bq.getRandomNumberInRange_int(0, graphsize - 1);

        src = 1;
        dest = 1999;

        if(src != dest){
            Monitor m = new Monitor();
            bq.onlineQueryTest(src,dest,false, m);

        }
//        bq.batchRandomQueries();
//        bq.query(65, 55, true);
//        bq.query(7732, 167, true);
    }

    public ArrayList<path> query(long src, long dest, boolean init) {
//        System.out.println(src+"   >>>>>>    "+dest);
        Monitor m = new Monitor();
        ArrayList<path> skylines = onlineQueryTest(src, dest, init, m);
//        System.out.println();
//        for (path p : skylines) {
//            System.out.println(p);
//        }

        System.out.println(skylines.size());

        return skylines;

    }

    public void batchRandomQueries() {
        for (int i = 0; i < 10; i++) {
            long startNode = getRandomNumberInRange(graphsize);
            long endNode = getRandomNumberInRange(graphsize);
            Monitor m1 = new Monitor();
            Monitor m2 = new Monitor();

            if (startNode != endNode) {
                onlineQueryTest(startNode, endNode, true, m1);
                onlineQueryTest(startNode, endNode, false, m2);
            }
        }
    }


    public ArrayList<path> onlineQueryTest(long startNode, long endNode, boolean init, Monitor monitor) {
        long start_ms = System.currentTimeMillis();

        BBSBaseline baseline = new BBSBaseline(this.graphsize, this.degree, 3);
//        BBSBaselineBusline baseline = new BBSBaselineBusline(this.graphsize, 28);
        baseline.results.clear();


        int sizeofinit = 0;
        if (init) {
            baseline.initilizeSkylinePath(startNode, endNode);
            sizeofinit = baseline.results.size();
            baseline.monitor.spInitTimeInBaseline = (System.currentTimeMillis() - start_ms);

        }

        baseline.queryOnline(startNode, endNode);
        long end_ms = System.currentTimeMillis();
        baseline.monitor.overallRuningtime = end_ms - start_ms;
        baseline.closeDB();

        monitor.clone(baseline.monitor);

        ArrayList<path> results = new ArrayList<>();
        results.addAll(baseline.results);
        System.out.println("# of node add to skyline function : "+ baseline.monitor.node_call_addtoskyline+"    # of add to skyline in final:"+baseline.monitor.callAddToSkyline);
        System.out.println("# dominated checking :"+ baseline.monitor.callcheckdominatedbyresult);
        System.out.println(baseline.monitor.getRunningtime_check_domination_resultByms());
        System.out.println(baseline.monitor.allsizeofthecheckdominatedbyresult);
        System.out.println(baseline.monitor.getOverallRuningtime_in_Sec());

        return results;
    }

    public long getRandomNumberInRange(long graphsize) {

        if (0 >= graphsize) {
            throw new IllegalArgumentException("the size of the graph must greater than 0");
        }

        return ThreadLocalRandom.current().nextLong(graphsize);
    }

    private int getRandomNumberInRange_int(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
    }

}
