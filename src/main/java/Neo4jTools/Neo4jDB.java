package Neo4jTools;

import configurations.ProgramProperty;
import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.io.IOException;

public class Neo4jDB {
    public GraphDatabaseService graphDB;
    private String DB_PATH;
    private ProgramProperty prop = new ProgramProperty();


    public Neo4jDB() {
        this.DB_PATH = prop.params.get("neo4jdb");
        System.out.println(this.DB_PATH);

    }

    public Neo4jDB(String subDBName) {
        this.DB_PATH = prop.params.get("neo4jdb")+"/"+subDBName;
        System.out.println(this.DB_PATH);

    }

    private static void registerShutdownHook(final GraphDatabaseService graphDb) {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }

    public void startDB() {
        GraphDatabaseBuilder builder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(this.DB_PATH));
        graphDB = builder.newGraphDatabase();
        if (this.graphDB != null) {
            System.out.println("Connect the neo4j db (" + this.DB_PATH + ") success !!!!");
        } else {
            System.out.println("Connect the neo4j db (" + this.DB_PATH + ") Failure !!!!");
        }
    }

    public void closeDB() {
        this.graphDB.shutdown();
        System.out.println("Close the neo4j db (" + this.DB_PATH + ") success !!!!");
    }

    public void deleleDB() {
        try {
            File f = new File(this.DB_PATH);
            if (f.exists()) {
                FileUtils.deleteDirectory(new File(this.DB_PATH));
                System.out.println("Delete the neo4j db (" + this.DB_PATH + ") success !!!!");
            }
        } catch (IOException e) {
//            e.printStackTrace();
            System.err.println("Delete the neo4j db (" + this.DB_PATH + ") fail !!!!");
        }
    }
}
