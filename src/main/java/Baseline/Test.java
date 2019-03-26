package Baseline;

import DataStructure.Monitor;
import Query.Index;
import Query.backbonePath;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class Test {
    public static void main(String args[]) {
        Test t = new Test();

//        t.batchRnadomTest(14);
//        t.test(20000);
        t.ResultTest(14, 9, 10);
    }

    private void ResultTest(long graphsize, long src, long dest) {
        Index i = new Index(graphsize, 3, 4);
        i.test(src, dest);
        for (backbonePath p : i.result) {
            System.out.println(p);
        }
        System.out.println("=========================");
        BaselineQuery bq = new BaselineQuery(graphsize);
        bq.query(src, dest, true);
        bq.query(src, dest, false);

    }

    private void batchRnadomTest(long graphsize) {
        for (int i = 0; i < 10; i++) {
            long startNode = getRandomNumberInRange(graphsize);
            long endNode = getRandomNumberInRange(graphsize);
            if (startNode != endNode) {
                runningTimeTest(graphsize, startNode, endNode);
            }
        }
    }


    public void runningTimeTest(long graphsize, long src, long dest) {
        long start_ms = System.currentTimeMillis();
        Index i = new Index(graphsize, 3, 4);
        long st_time = System.currentTimeMillis();
        i.test(src, dest);
        long ed_time = System.currentTimeMillis();
        i.monitor.overallRuningtime = ed_time - start_ms;
        i.monitor.indexQueryTime = ed_time - st_time;


        Monitor index_monitor = i.monitor;

        Monitor base_monitor_1 = new Monitor();
        Monitor base_monitor_2 = new Monitor();

        BaselineQuery bq = new BaselineQuery(graphsize);
        bq.onlineQueryTest(src, dest, true, base_monitor_1);
        bq.onlineQueryTest(src, dest, false, base_monitor_2);


        StringBuilder sb = new StringBuilder();
        sb.append(src + "  >>>  " + dest + "|");
        sb.append(base_monitor_2.overallRuningtime).append(" ").append(base_monitor_1.overallRuningtime).append(" ").append(index_monitor.indexQueryTime).append(" ").append(index_monitor.overallRuningtime).append(" | ");
        sb.append(base_monitor_2.node_call_addtoskyline).append(" ").append(base_monitor_1.node_call_addtoskyline).append(" ").append(index_monitor.callAddToSkyline).append(" | ");
        sb.append(base_monitor_2.callAddToSkyline).append(" ").append(base_monitor_1.callAddToSkyline).append(" ").append(index_monitor.finnalCallAddToSkyline).append(" | ");
        sb.append(base_monitor_2.callcheckdominatedbyresult).append(" ").append(base_monitor_1.callcheckdominatedbyresult).append(" ").append(index_monitor.callcheckdominatedbyresult).append(" | ");
        System.out.println(sb);
    }

    private void test(long graphsize) {
        ArrayList<Pair<Long, Long>> queries = new ArrayList<>();
        queries.add(new Pair<>(18229L, 17123L));
        queries.add(new Pair<>(5190L, 2895L));
        queries.add(new Pair<>(19874L, 1404L));
        queries.add(new Pair<>(10703L, 1284L));
        queries.add(new Pair<>(4516L, 6273L));
        queries.add(new Pair<>(576L, 10196L));
        queries.add(new Pair<>(18602L, 1244L));
        queries.add(new Pair<>(16593L, 13580L));
        queries.add(new Pair<>(445L, 16010L));
        queries.add(new Pair<>(11246L, 8453L));

        for (Pair<Long, Long> query : queries) {
            long src = query.getKey();
            long dest = query.getValue();
            runningTimeTest(graphsize, src, dest);

        }
    }

    public long getRandomNumberInRange(long graphsize) {

        if (0 >= graphsize) {
            throw new IllegalArgumentException("the size of the graph must greater than 0");
        }

        return ThreadLocalRandom.current().nextLong(graphsize);
    }
}
