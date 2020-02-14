package v5LinkedList.clusterversion;

import java.util.HashMap;
import java.util.HashSet;


public class NodeCluster {
    int cluster_id;
    int max_size = 100;
    HashSet<Long> node_list = new HashSet<>();

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
}

class NodeClusters {
    HashMap<Integer, NodeCluster> clusters = new HashMap<>();

    public NodeClusters() {
        //add a noise cluster to store the noise nodes
        NodeCluster noise_cluster = new NodeCluster(-1);
        this.clusters.put(-1, noise_cluster);

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
        return clusters.size() - 1;
    }
}


