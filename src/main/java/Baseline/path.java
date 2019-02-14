package Baseline;


import Neo4jTools.Line;
import Neo4jTools.Neo4jDB;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;
import java.util.Iterator;


public class path {
    public double[] costs;
    public boolean expaned;
    public long startNode, endNode;

//    public ArrayList<Long> nodes;
//    public ArrayList<Long> rels;
    public ArrayList<String> propertiesName;


    public path(myNode current) {
        this.costs = new double[3];
        costs[0] = costs[1] = costs[2] = 0;
        this.startNode = current.id;
        this.endNode = current.id;
        this.expaned = false;
        this.propertiesName = new ArrayList<>();
        this.setPropertiesName(current.neo4j);
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


    public void setPropertiesName(Neo4jDB neo4j) {
        this.propertiesName = neo4j.propertiesName;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(this.startNode+"-->"+this.endNode+",[");
        for (double d : this.costs) {
            sb.append(" " + d);
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {

        if (obj == null && this == null) {
            return true;
        } else if ((obj == null && this != null) || (obj != null && this == null)) {
            return false;
        }

        if (obj == this)
            return true;
        if (!(obj instanceof path))
            return false;


        path o_path = (path) obj;
        if (o_path.endNode != endNode || o_path.startNode != startNode) {
            return false;
        }

        for (int i = 0; i < costs.length; i++) {
            if (o_path.costs[i] != costs[i]) {
                return false;
            }
        }

//        if (!o_path.nodes.equals(this.nodes) || !o_path.rels.equals(this.rels)) {
//            return false;
//        }
        return true;
    }
}
