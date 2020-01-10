package Baseline;

import DataStructure.Monitor;
import Query.IndexAccuracy;
import Query.backbonePath;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class Test {
    public static void main(String args[]) {
        Test t = new Test();
        t.test(1000,false);

//        t.batchRnadomTest(1000, false);
//        t.test(20000);
//        t.ResultTest(1000, 4,22, 25, true);

//        t.pairwiseResultTest(1000, 4, true);
    }

    private void pairwiseResultTest(long graphsize, int degree, boolean readIntraIndex) {
        for (long src = 0; src < graphsize - 1; src++) {
            for (long dest = src; dest < graphsize; dest++) {
                if (src != dest) {
                    boolean same = compareMethods(graphsize, degree, src, dest, readIntraIndex);
                    System.out.println(src + " ----->>  " + dest + "  " + same);
                }
            }

            System.out.println("============================");
        }
    }

    private boolean compareMethods(long graphsize, int degree, long src, long dest, boolean readIntraIndex) {
        IndexAccuracy i = new IndexAccuracy(graphsize, 3, degree, readIntraIndex);
        i.test(src, dest, readIntraIndex);
        BaselineQuery bq = new BaselineQuery(graphsize, degree);
        ArrayList<path> baseline_result = bq.query(src, dest, true);
        comResultObj sameResult = compareResult(i.result, baseline_result);
        return sameResult.isSame;
    }

    private comResultObj compareResult(ArrayList<backbonePath> result, ArrayList<path> baseline_result) {

        comResultObj compare_result = new comResultObj(result.size(), baseline_result.size());

        boolean same = true;
        int i = 0;

        for (backbonePath bp : result) {
            double min = Double.MAX_VALUE;
            boolean flag = false;
            for (path p : baseline_result) {
                double distance = Math.sqrt(Math.pow(bp.costs[0] - p.costs[0], 2) + Math.pow(bp.costs[1] - p.costs[1], 2) + Math.pow(bp.costs[2] - p.costs[2], 2));

                if (distance < min) {
                    if (Math.abs(distance) < 0.01) {
                        min = 0;
                    } else {
                        min = distance;
                    }
                }
            }

            if (min == 0) {
                flag = true;
            }

            compare_result.min_distance[i] = min;
            compare_result.isSame_result[i] = flag;


            if (!flag & same) {
                same = false;
            }

            i++;
        }

        compare_result.isSame = same;

        return compare_result;

    }

    private void ResultTest(long graphsize, int degree, long src, long dest, boolean readIntraIndex) {
        IndexAccuracy i = new IndexAccuracy(graphsize, 3, degree, readIntraIndex);
        i.test(src, dest, readIntraIndex);
        for (backbonePath p : i.result) {
            System.out.println(p);
        }
        System.out.println("=========================");
        BaselineQuery bq = new BaselineQuery(graphsize, degree);
        ArrayList<path> baseline_result1 = bq.query(src, dest, true);
        ArrayList<path> baseline_result2 = bq.query(src, dest, false);

        for (path p : baseline_result1) {
            System.out.println(p);
        }
        System.out.println("=========================");

        System.out.println(compareResult(i.result, baseline_result1));
        System.out.println(compareResult(i.result, baseline_result2));

    }

    private void batchRnadomTest(long graphsize, boolean readIntraIndex) {
        for (int i = 0; i < 40; i++) {
            long startNode = getRandomNumberInRange(graphsize);
            long endNode = getRandomNumberInRange(graphsize);
            if (startNode != endNode) {
                runningTimeTest(graphsize, startNode, endNode, readIntraIndex);
            }
        }
    }


    public void runningTimeTest(long graphsize, long src, long dest, boolean readIntraIndex) {
        long start_ms = System.currentTimeMillis();
        IndexAccuracy i = new IndexAccuracy(graphsize, 3, 4, readIntraIndex);
        long st_time = System.currentTimeMillis();
        i.test(src, dest, readIntraIndex);
        long ed_time = System.currentTimeMillis();
        i.monitor.overallRuningtime = ed_time - start_ms;
        i.monitor.indexQueryTime = ed_time - st_time;

        Monitor index_monitor = i.monitor;

        Monitor base_monitor_1 = new Monitor();
        Monitor base_monitor_2 = new Monitor();

        BaselineQuery bq = new BaselineQuery(graphsize);
        ArrayList<path> r1 = bq.onlineQueryTest(src, dest, true, base_monitor_1);
        ArrayList<path> r2 = bq.onlineQueryTest(src, dest, false, base_monitor_2);


        StringBuilder sb = new StringBuilder();
        sb.append(src + "  >>>  " + dest + " | ");
        sb.append(base_monitor_2.overallRuningtime).append(" ").append(base_monitor_1.overallRuningtime).append(" ").append(index_monitor.indexQueryTime).append(" ").append(index_monitor.overallRuningtime).append(" ").append(index_monitor.overallRuningtime - index_monitor.indexQueryTime).append(" | ");
        sb.append(base_monitor_2.node_call_addtoskyline).append(" ").append(base_monitor_1.node_call_addtoskyline).append(" ").append(index_monitor.callAddToSkyline).append(" | ");
        sb.append(base_monitor_2.callAddToSkyline).append(" ").append(base_monitor_1.callAddToSkyline).append(" ").append(index_monitor.finnalCallAddToSkyline).append(" | ");
        sb.append(base_monitor_2.callcheckdominatedbyresult).append(" ").append(base_monitor_1.callcheckdominatedbyresult).append(" ").append(index_monitor.callcheckdominatedbyresult).append(" | ");
        sb.append(compareResult(i.result, r1)).append(" ");
        System.out.println(sb);
    }

    private void test(long graphsize, boolean readIntraIndex) {
        ArrayList<Pair<Long, Long>> queries = new ArrayList<>();
        queries.add(new Pair<>(242L,121L));
        queries.add(new Pair<>(619L,38L));
        queries.add(new Pair<>(904L,892L));
        queries.add(new Pair<>(82L,64L));
        queries.add(new Pair<>(174L,499L));
        queries.add(new Pair<>(454L,821L));
        queries.add(new Pair<>(904L,828L));
        queries.add(new Pair<>(994L,337L));
        queries.add(new Pair<>(834L,741L));
        queries.add(new Pair<>(638L,229L));
        queries.add(new Pair<>(262L,922L));
        queries.add(new Pair<>(358L,714L));
        queries.add(new Pair<>(562L,575L));
        queries.add(new Pair<>(133L,112L));
        queries.add(new Pair<>(639L,242L));
        queries.add(new Pair<>(468L,41L));
        queries.add(new Pair<>(243L,908L));
        queries.add(new Pair<>(351L,280L));
        queries.add(new Pair<>(518L,64L));
        queries.add(new Pair<>(145L,701L));
        queries.add(new Pair<>(653L,453L));
        queries.add(new Pair<>(403L,167L));
        queries.add(new Pair<>(120L,809L));
        queries.add(new Pair<>(671L,937L));
        queries.add(new Pair<>(305L,450L));
        queries.add(new Pair<>(932L,666L));
        queries.add(new Pair<>(155L,328L));
        queries.add(new Pair<>(414L,108L));
        queries.add(new Pair<>(143L,8L));
        queries.add(new Pair<>(301L,587L));
        queries.add(new Pair<>(764L,302L));
        queries.add(new Pair<>(305L,252L));
        queries.add(new Pair<>(39L,359L));
        queries.add(new Pair<>(651L,548L));
        queries.add(new Pair<>(95L,649L));
        queries.add(new Pair<>(994L,881L));
        queries.add(new Pair<>(360L,859L));
        queries.add(new Pair<>(152L,129L));
        queries.add(new Pair<>(325L,960L));
        queries.add(new Pair<>(104L,359L));


        for (Pair<Long, Long> query : queries) {
            long src = query.getKey();
            long dest = query.getValue();
            runningTimeTest(graphsize, src, dest, readIntraIndex);

        }
    }

    public long getRandomNumberInRange(long graphsize) {

        if (0 >= graphsize) {
            throw new IllegalArgumentException("the size of the graph must greater than 0");
        }

        return ThreadLocalRandom.current().nextLong(graphsize);
    }


    class comResultObj {
        public boolean[] isSame_result;
        int number_app_results = 0;
        int number_baseline_results = 0;
        boolean isSame = false;

        double[] min_distance;

        public comResultObj(int number_app_results, int number_baseline_results) {
            this.number_app_results = number_app_results;
            this.number_baseline_results = number_baseline_results;
            min_distance = new double[number_app_results];
            isSame_result = new boolean[number_app_results];
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append("comResultObj:" + isSame+"    ");


            int same_num = 0;
            for (boolean s : this.isSame_result) {
                if (s) {
                    same_num++;
                }
            }

            sb.append(same_num+"/"+this.number_app_results+"/"+number_baseline_results+"    ");

            for (double d : min_distance) {
                sb.append(d).append(",");
            }

            return sb.toString().substring(0,sb.toString().lastIndexOf(","));
        }
    }
}
