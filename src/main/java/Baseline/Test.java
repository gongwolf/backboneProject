package Baseline;

import Query.Index;
import javafx.util.Pair;
import java.util.ArrayList;

public class Test {
    public static void main(String args[]){
        Test t = new Test();
        t.test();
    }

    private void test() {
        ArrayList<Pair<Long,Long>> queries = new ArrayList<>();
        queries.add(new Pair<>(7732L,167L));
        queries.add(new Pair<>(19978L,16292L));
        queries.add(new Pair<>(3675L,13091L));
        queries.add(new Pair<>(2771L,8109L));
        queries.add(new Pair<>(7202L,12618L));
        queries.add(new Pair<>(1446L,11101L));
        queries.add(new Pair<>(9751L,19905L));
        queries.add(new Pair<>(13653L,10836L));
        queries.add(new Pair<>(16808L,14333L));
        queries.add(new Pair<>(1402L,10787L));

        int graphsize = 40000;

        for(Pair<Long, Long> query:queries){
            long src = query.getKey();
            long dest = query.getValue();

            long start_ms = System.currentTimeMillis();
            Index i = new Index(graphsize, 3, 4);
            long st_time = System.currentTimeMillis();
            i.test(src, dest);
            long ed_time = System.currentTimeMillis();
            long end_ms = System.currentTimeMillis();
            long index_overall_rt = end_ms-start_ms;
            long index_query_rt = ed_time-st_time;
            BaselineQuery bq = new BaselineQuery(graphsize);
            bq.query(src, dest, true);
            bq.query(src, dest, false);
            System.out.println("  "+index_query_rt+"  "+index_overall_rt);
            System.out.println("==========================================================");
        }
    }
}
