package v5LinkedList.clusterversion;

import Neo4jTools.Neo4jDB;
import org.neo4j.graphdb.Transaction;
import sun.font.DelegatingShape;

import java.util.*;


public class NodeCluster {
    int cluster_id;
    int max_size = 300;

    HashSet<Long> node_list = new HashSet<>();
    HashSet<Long> border_node_list = new HashSet<>();
    public ArrayList<Long> list_b;
    public HashSet<Long> rels = new HashSet<>();

    public NodeCluster(int id) {
        this.cluster_id = id;
    }

    public boolean isInCluster(long node_id) {
        return node_list.contains(node_id);
    }

    public void addToCluster(long new_node_id) {
        this.node_list.add(new_node_id);
    }

    public boolean oversize() {
        return this.node_list.size() >= max_size;
    }

    public void updateBorderList(Neo4jDB neo4j) {

        this.border_node_list.clear();
        this.list_b.clear();

        try (Transaction tx = neo4j.graphDB.beginTx()) {
            for (long node_id : node_list) {
                boolean connect_to_other_cluster = false;
                ArrayList<Long> n_list = neo4j.getNeighborsIdList(node_id);
                for (long n : n_list) {
                    if (!this.node_list.contains(n)) {
                        connect_to_other_cluster = true;
                        break;
                    }
                }

                if (connect_to_other_cluster) {
                    this.border_node_list.add(node_id);
                }
            }
            tx.success();
        }
        list_b = new ArrayList<>(this.border_node_list);
    }

    public HashSet<Long> getBorderList() {
        return this.border_node_list;
    }

    public Long getRandomBorderNode() {
        return list_b.get(getRandomNumberInRange(0, this.border_node_list.size() - 1));
    }

    private static int getRandomNumberInRange(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
    }

    public boolean oversize(int cluster_size) {
        double threshold = Math.max(cluster_size, max_size);
        return this.node_list.size() >= threshold;
    }

    public void addAll(NodeCluster other) {
        cluster_id = other.cluster_id;
        max_size = other.max_size;

        node_list = new HashSet<>(other.node_list);
        border_node_list = new HashSet<>(other.border_node_list);
        list_b = new ArrayList<>(other.list_b);
    }

    public void addRels(HashSet<Long> rels) {
        this.rels.addAll(rels);
    }
}

class NodeClusters {
    HashMap<Integer, NodeCluster> clusters = new HashMap<>();

    public NodeClusters() {
        //add a noise cluster to store the noise nodes
        NodeCluster noise_cluster = new NodeCluster(0);
        this.clusters.put(noise_cluster.cluster_id, noise_cluster);

    }

    public boolean isInClusters(long node_id) {
        for (int cluster_id : clusters.keySet()) {
            if (this.clusters.get(cluster_id).isInCluster(node_id)) {
                return true;
            }
        }

        return false;
    }

    public int getNextClusterID() {
        return clusters.size();
    }

    public int getClusterIdByRelId(Long rel_id) {
        for (int cluster_id : clusters.keySet()) {
            if (this.clusters.get(cluster_id).isInCluster(rel_id)) {
                return cluster_id;
            }
        }
        return -1;
    }

    public int getNumberOfClusters() {
        return this.clusters.size();
    }
}


