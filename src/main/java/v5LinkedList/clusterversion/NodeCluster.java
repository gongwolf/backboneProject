package v5LinkedList.clusterversion;

import java.util.HashMap;
import java.util.HashSet;


public class NodeCluster {
    int cluster_id;
    HashSet<Long> node_list = new HashSet<>();
}

class NodeClusters {
    HashMap<Integer, NodeCluster> clusters = new HashMap<>();

    public boolean isInClusters(long node_id) {
        for (int cluster_id : clusters.keySet()) {
            for (long target_node_id : this.clusters.get(cluster_id).node_list) {
                if (target_node_id == node_id) {
                    return true;
                }
            }
        }

        return false;
    }
}


