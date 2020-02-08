package Query.test;

import Query.landmark.LandmarkBBS;

public class LandmarkBuild {
    public static void main(String args[]) {
        LandmarkBuild ldb = new LandmarkBuild();
        ldb.test();
    }

    private void test() {
        String sub_db_name = "sub_ny_USA_Level" + 8;
        LandmarkBBS layer_bbs = new LandmarkBBS(sub_db_name);
        layer_bbs.readLandmarkIndex(5, null, true);
        layer_bbs.neo4j.closeDB();
    }
}
