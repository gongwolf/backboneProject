package test;

import Neo4jTools.Neo4jDB;

public class testDBConnection {
    public static void main(String args[]) {
        String dbname = 10000 + "_" + 4 + "_" + 3 + "_Level0";
        Neo4jDB neo4jdb = new Neo4jDB(dbname);
        neo4jdb.startDB(false);
        System.out.println(neo4jdb.DB_PATH);
        System.out.println(neo4jdb.graphDB);
        System.out.println(neo4jdb.getNumberofEdges());

        neo4jdb.closeDB();
    }
}
