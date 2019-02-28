package Baseline;

public class BaselineQuery {

    public static void main(String arg[]){

        long startNode = 9;
        long endNode = 999;

        BaselineQuery bq = new BaselineQuery();
        bq.onlineQueryTest(startNode,endNode);

    }



    public void onlineQueryTest(long startNode, long endNode){
        BBSBaseline baseline = new BBSBaseline(1000,4,3);

        baseline.results.clear();
        baseline.initilizeSkylinePath(startNode,endNode);

        System.out.println(baseline.results.size());
        for(path p:baseline.results){
            System.out.println(p);
        }

        baseline.closeDB();
    }
}
