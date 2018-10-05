package test;

import Neo4jTools.Neo4jDB;

public class testDB {
    public static void main(String args[]) {
        testDB t = new testDB();
        t.test();
    }

    private void test() {
        Neo4jDB conn = new Neo4jDB();
        conn.deleleDB();
        conn.startDB();
        conn.closeDB();
    }
}
