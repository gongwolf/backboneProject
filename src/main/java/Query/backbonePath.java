package Query;

import Neo4jTools.Line;
import Neo4jTools.Neo4jDB;
import Query.landmark.path;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

public class backbonePath {
    public boolean hasCycle;
    public long source;
    public long destination;
    public double[] costs;
    public ArrayList<Long> highwayList = new ArrayList<>();
    public ArrayList<String> propertiesName = new ArrayList<>();
    public path p = null; // skyline paths on the highest level
    public boolean expanded = false;

    public backbonePath(long node_id) {
        this.source = node_id;
        this.destination = node_id;
        costs = new double[3];
        costs[0] = costs[1] = costs[2] = 0;
        this.highwayList.add(node_id);
    }

    //used in lower level index construction (except the highest lvevel), the attribute p is null.
    public backbonePath(long h_node, double[] costs, backbonePath old_path) {
        this.source = old_path.source;
        this.destination = h_node;

        this.highwayList.clear();
        this.highwayList.addAll(old_path.highwayList);
        this.highwayList.add(h_node);

        calculatedCosts(costs, old_path.costs);

        if (old_path.p != null) {
            this.p = new path(old_path.p);
            this.p.expanded = false;
            p.rels.add(null);
        }

        if (Collections.frequency(highwayList, destination) >= 2) {
            this.hasCycle = true;
        }
    }

    public backbonePath(backbonePath s_t_h_bpath, backbonePath d_t_h_bpath) {
        this.source = s_t_h_bpath.source;
        this.destination = d_t_h_bpath.source;

        this.highwayList.clear();
        this.highwayList.addAll(s_t_h_bpath.highwayList);

        ArrayList<Long> reversed_highway = new ArrayList<>(d_t_h_bpath.highwayList);
        Collections.reverse(reversed_highway);


        costs = new double[3];
        calculatedCosts(s_t_h_bpath.costs, d_t_h_bpath.costs);

        if (highwayList.contains(this.destination)) {
            this.hasCycle = true;
        }

        if (s_t_h_bpath.p != null) {
            this.p = new path(s_t_h_bpath.p);
            this.p.expanded = false;
        }

        int last_index = this.highwayList.size() - 1;

        this.highwayList.remove(this.highwayList.size() - 1);
        this.highwayList.addAll(reversed_highway);

        if (s_t_h_bpath.p != null) {
            for (int idx = last_index; idx < highwayList.size() - 1; idx++) {
                p.rels.add(null);
            }
        }

        if (Collections.frequency(highwayList, destination) >= 2) {
            this.hasCycle = true;
        }

    }

    public backbonePath(backbonePath s_t_h_bpath, backbonePath d_t_h_bpath, double[] costsInHighestLevel) {
        this.source = s_t_h_bpath.source;
        this.destination = d_t_h_bpath.source;

        this.highwayList.clear();
        this.highwayList.addAll(s_t_h_bpath.highwayList);

        ArrayList<Long> reversed_highway = new ArrayList<>(d_t_h_bpath.highwayList);
        Collections.reverse(reversed_highway);

        this.highwayList.addAll(reversed_highway);

        costs = new double[3];
        calculatedCosts(s_t_h_bpath.costs, d_t_h_bpath.costs);
        calculatedCosts(this.costs, costsInHighestLevel);

        if (Collections.frequency(highwayList, destination) >= 2) {
            this.hasCycle = true;
        }

    }

    //the backbone paths in the highest level, used when the pari-wised index at the highest level are built
    public backbonePath(long sid, long did, double[] costs) {
        this.source = sid;
        this.destination = did;
        this.costs = new double[3];
        this.highwayList = new ArrayList<>();

        this.highwayList.add(source);
        this.highwayList.add(destination);

        System.arraycopy(costs, 0, this.costs, 0, this.costs.length);
        this.p = null;
    }

    /**
     * Only used create the backbone path for the source highway node in the landmark bbs process
     *
     * @param bp the backbone path from source node of the query to the source highway nodes
     * @param dp the path object is represent the backbone path dp
     */
    public backbonePath(backbonePath bp, path dp, Neo4jDB neo4j) {

        this.costs = new double[3];


        this.source = bp.source;
        this.destination = bp.destination;

        this.highwayList.clear();
        this.highwayList.addAll(bp.highwayList);

        System.arraycopy(bp.costs, 0, this.costs, 0, this.costs.length);

        this.setPropertiesName(neo4j);
        this.p = dp;

        if (Collections.frequency(highwayList, destination) >= 2) {
            this.hasCycle = true;
        }
    }

    /**
     * Create the new backbone on the expansion process, used in the BBS
     * the attribute p is not null
     *
     * @param old_bp the old backbone path
     * @param np     the path object
     * @param rel    the relationship is used to expand the backbone and the path
     */
    public backbonePath(backbonePath old_bp, path np, Relationship rel, Neo4jDB neo4j) {
        this.costs = new double[3];

        this.source = old_bp.source;
        this.destination = rel.getOtherNodeId(old_bp.destination);

        this.highwayList.clear();
        this.highwayList.addAll(old_bp.highwayList);
        this.highwayList.add(this.destination);

        this.setPropertiesName(neo4j);
        System.arraycopy(old_bp.costs, 0, this.costs, 0, this.costs.length);
        calculateCosts(rel, neo4j);
        this.p = np;

        if (Collections.frequency(highwayList, destination) >= 2) {
            this.hasCycle = true;
        }

    }

    public backbonePath(WeightedPath paths) {
        this.costs = new double[3];
        this.source = paths.startNode().getId();
        this.destination = paths.endNode().getId();

        this.highwayList = new ArrayList<>();
        Iterator<Node> nodes_iter = paths.nodes().iterator();
        while (nodes_iter.hasNext()) {
            this.highwayList.add(nodes_iter.next().getId());
        }

        this.p = new path();
        Iterator<Relationship> rel_iter = paths.relationships().iterator();
        while (rel_iter.hasNext()) {
            this.p.rels.add(rel_iter.next().getId());
        }

        this.p.expanded = false;
        this.p.possible_destination = new HashMap<>();

    }

    public backbonePath(backbonePath s_t_h_bpath, backbonePath d_t_h_bpath, boolean reverse_the_second_part) {
        this.source = s_t_h_bpath.source;
        this.destination = d_t_h_bpath.source;

        this.highwayList.clear();
        this.highwayList.addAll(s_t_h_bpath.highwayList);

        ArrayList<Long> reversed_highway = new ArrayList<>(d_t_h_bpath.highwayList);
        if (reverse_the_second_part) {
            Collections.reverse(reversed_highway);
        }

        costs = new double[3];
        calculatedCosts(s_t_h_bpath.costs, d_t_h_bpath.costs);

        if (highwayList.contains(this.destination)) {
            this.hasCycle = true;
        }

        if (s_t_h_bpath.p != null) {
            this.p = new path(s_t_h_bpath.p);
            this.p.expanded = false;
        }

        int last_index = this.highwayList.size() - 1;

        this.highwayList.remove(this.highwayList.size() - 1);
        this.highwayList.addAll(reversed_highway);

        if (s_t_h_bpath.p != null) {
            for (int idx = last_index; idx < highwayList.size() - 1; idx++) {
                p.rels.add(null);
            }
        }

        if (Collections.frequency(highwayList, destination) >= 2) {
            this.hasCycle = true;
        }
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(source + " >>> " + destination + " [" + costs[0] + "," + costs[1] + "," + costs[2] + "] ");
        if (p != null) {
            for (int i = 0; i < this.highwayList.size() - 1; i++) {
                sb.append("(" + this.highwayList.get(i) + ")");
                sb.append("--[" + this.p.rels.get(i) + "]-->");
            }
            sb.append("(" + this.highwayList.get(highwayList.size() - 1) + ")");
        } else {
            sb.append(" " + highwayList);
        }
        return sb.toString();
    }

    private void calculatedCosts(double[] new_costs, double[] old_costs) {
        this.costs = new double[3];
        for (int i = 0; i < this.costs.length; i++) {
            this.costs[i] = old_costs[i] + new_costs[i];
        }
    }

    private void calculatedCosts(double[] source_costs, double[] bbs_costs, double[] dest_costs) {
        this.costs = new double[3];
        for (int i = 0; i < this.costs.length; i++) {
            this.costs[i] = source_costs[i] + bbs_costs[i] + dest_costs[i];
        }
    }

    private void calculateCosts(Relationship rel, Neo4jDB neo4j) {
        try (Transaction tx = neo4j.graphDB.beginTx()) {
            if (this.source != this.destination) {
                int i = 0;
                for (String pname : this.propertiesName) {
                    this.costs[i] = this.costs[i] + (double) rel.getProperty(pname);
                    i++;
                }
            }
            tx.success();
        }
    }

    public void setPropertiesName(Neo4jDB neo4j) {
        this.propertiesName = neo4j.propertiesName;
    }

    public ArrayList<backbonePath> expand(Neo4jDB neo4j) {
        ArrayList<backbonePath> result = new ArrayList<>();
        try (Transaction tx = neo4j.graphDB.beginTx()) {
            Iterable<Relationship> rels = neo4j.graphDB.getNodeById(this.destination).getRelationships(Line.Linked, Direction.BOTH);
            Iterator<Relationship> rel_Iter = rels.iterator();
            while (rel_Iter.hasNext()) {
                Relationship rel = rel_Iter.next();
                path nPath = new path(p, rel);
                backbonePath nBPath = new backbonePath(this, nPath, rel, neo4j);
                result.add(nBPath);
            }
            tx.success();
        }
        return result;
    }
}
