package Neo4jTools;

import org.neo4j.graphdb.GraphDatabaseService;

public class convertNeo4jDBtoTextfile {
    public static void main(String args[]) {
        convertNeo4jDBtoTextfile cnt = new convertNeo4jDBtoTextfile();
        cnt.convert();
    }

    private void convert() {

        String number_of_node_str = 50 + "K";

        String sub_db_name = "sub_ny_USA_" + number_of_node_str + "_Level0";
//        sub_db_name = "sub_ny_USA_Level0";
        String prefix = "/home/gqxwolf/mydata/projectData/BackBone/";
//        String textFilePath = prefix + "busline_sub_graph_NY/level1";
        String textFilePath = prefix + "busline_sub_graph_NY_" + number_of_node_str + "/level0/";

        Neo4jDB neo4j = new Neo4jDB(sub_db_name);
        neo4j.startDB(false);
        GraphDatabaseService graphdb = neo4j.graphDB;
        long pre_n = neo4j.getNumberofNodes();
        long pre_e = neo4j.getNumberofEdges();
        System.out.println("deal with the database (" + sub_db_name + ") graph at " + neo4j.DB_PATH + "  " + pre_n + " nodes and " + pre_e + " edges");

        neo4j.saveGraphToTextFormation(textFilePath);
        neo4j.closeDB();
    }
}
