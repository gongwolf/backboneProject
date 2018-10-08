package Index;

import Neo4jTools.Neo4jDB;
import javafx.util.Pair;
import org.neo4j.graphdb.*;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

public class Index {

    GraphDatabaseService graphdb;


    int graph_size = 1000;
    double samnode_t = 7;

    TreeMap<Pair<Integer, Integer>, Integer> degree_pairs = new TreeMap(new PairComparator());

    public static void main(String args[]) {
        Index i = new Index();
        i.indexBuild();
    }

    private void indexBuild() {
        int level = 0;
        String sub_db_name = graph_size + "-" + samnode_t + "-" + "Lvevl" + level;
        Neo4jDB neo4j = new Neo4jDB(sub_db_name);
        neo4j.startDB();
        graphdb = neo4j.graphDB;
        getDegreePairs();

        neo4j.closeDB();
    }

    private void getDegreePairs() {
        this.degree_pairs.clear();
        try (Transaction tx = graphdb.beginTx()) {
            ResourceIterable<Relationship> rels = this.graphdb.getAllRelationships();
            ResourceIterator<Relationship> rels_iter = rels.iterator();
            while (rels_iter.hasNext()) {
                Relationship r = rels_iter.next();
                int start_r = r.getStartNode().getDegree(Direction.BOTH);
                int end_r = r.getEndNode().getDegree(Direction.BOTH);
                Pair<Integer, Integer> p = new Pair<>(start_r, end_r);
                if (this.degree_pairs.containsKey(p)) {
                    this.degree_pairs.put(p, this.degree_pairs.get(p) + 1);
                } else {
                    this.degree_pairs.put(p, 1);
                }

            }
            tx.success();
        }

        for (Map.Entry<Pair<Integer, Integer>, Integer> e : degree_pairs.entrySet()) {
            if (e.getKey().getKey() == 1 || e.getKey().getValue() == 1) {
                System.err.println(e.getKey().getKey() + "  " + e.getKey().getValue() + "== " + (e.getKey().getKey() + e.getKey().getValue()) + " : " + e.getValue());
            } else {
                System.out.println(e.getKey().getKey() + "  " + e.getKey().getValue() + "== " + (e.getKey().getKey() + e.getKey().getValue()) + " : " + e.getValue());
            }
        }
        System.out.println(this.degree_pairs.size());
    }
}


class PairComparator implements Comparator<Pair<Integer, Integer>> {
    @Override
    public int compare(Pair<Integer, Integer> o1, Pair<Integer, Integer> o2) {
        if ((o1.getKey() + o1.getValue()) - (o2.getKey() + o2.getValue()) != 0) {
            return (o1.getKey() + o1.getValue()) - (o2.getKey() + o2.getValue());
        } else {
            return o1.getKey() - o2.getKey();
        }//sort in descending order
    }
}