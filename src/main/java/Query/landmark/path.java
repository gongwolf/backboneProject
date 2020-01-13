package Query.landmark;

import Neo4jTools.Line;
import Neo4jTools.Neo4jDB;
import Query.backbonePath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class path {

    public double[] costs;
    public boolean expaned;
    public long startNode, endNode;

    public ArrayList<Long> nodes;
    public ArrayList<Long> rels;
    public ArrayList<String> propertiesName;
    public HashMap<Long, ArrayList<backbonePath>> possible_destination;


    /**
     * initialize the path by using the backbone path from sid to highway at the highest level
     *
     * @param bp                           the backbone path from sid to highway node at the highest level
     * @param neo4j                        the neo4j object to find the property names
     * @param destination_highways_results the list of destination node with its skyline paths, the possible last components (destination highway to destination node) could form the final results.
     */
    public path(backbonePath bp, Neo4jDB neo4j, HashMap<Long, ArrayList<backbonePath>> destination_highways_results) {
        this.costs = new double[3];
        this.startNode = bp.source;
        this.endNode = bp.destination;

        this.expaned = false;
        this.propertiesName = new ArrayList<>();
        this.setPropertiesName(neo4j);

        this.nodes = new ArrayList<>();
        this.nodes.addAll(bp.highwayList);

        this.rels = new ArrayList<>();
        for (int i = 0; i < nodes.size()-1; i++) {
            rels.add(null);
        }

        costs[0] = bp.costs[0];
        costs[1] = bp.costs[1];
        costs[2] = bp.costs[2];

        possible_destination = new HashMap<>(destination_highways_results);
    }


    public path(path old_path, Relationship rel) {
        this.costs = new double[3];
        this.startNode = old_path.startNode;
        this.endNode = rel.getOtherNodeId(old_path.endNode);
//        System.out.println("            create new path "+this.startNode+"   "+this.endNode);
        this.propertiesName = new ArrayList<>(old_path.propertiesName);
        expaned = false;
        System.arraycopy(old_path.costs, 0, this.costs, 0, this.costs.length);
        calculateCosts(rel);


        this.nodes = new ArrayList<>();
        this.rels = new ArrayList<>();
        this.nodes.addAll(old_path.nodes);
        this.nodes.add(rel.getOtherNodeId(nodes.get(nodes.size() - 1)));
        this.rels.addAll(old_path.rels);
        this.rels.add(rel.getId());

        possible_destination = new HashMap<>(old_path.possible_destination);
    }


    public void setPropertiesName(Neo4jDB neo4j) {
        this.propertiesName = neo4j.propertiesName;
    }


    public ArrayList<path> expand(Neo4jDB neo4j) {
        ArrayList<path> result = new ArrayList<>();
        try (Transaction tx = neo4j.graphDB.beginTx()) {
            Iterable<Relationship> rels = neo4j.graphDB.getNodeById(this.endNode).getRelationships(Line.Linked, Direction.BOTH);
            Iterator<Relationship> rel_Iter = rels.iterator();
            while (rel_Iter.hasNext()) {
                Relationship rel = rel_Iter.next();
                path nPath = new path(this, rel);
                result.add(nPath);
            }
            tx.success();
        }
        return result;
    }

    private void calculateCosts(Relationship rel) {
        if (this.startNode != this.endNode) {
            int i = 0;
            for (String pname : this.propertiesName) {
                this.costs[i] = this.costs[i] + (double) rel.getProperty(pname);
                i++;
            }
        }
    }
}
