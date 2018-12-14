package Index;

import java.util.ArrayList;

public class ChainComponet {
    ArrayList<Long> nodes = new ArrayList<>();
    ArrayList<Long> edges = new ArrayList<>();

    public void addNode(long nodeID) {
        if (!nodes.contains(nodeID)) {
            nodes.add(nodeID);
        }
    }

    public void addEdge(long relID) {
        if (!edges.contains(relID)) {
            edges.add(relID);
        }
    }

    @Override
    public String toString() {
        StringBuilder nsb = new StringBuilder();
        nsb.append("[node: ");
        for (long nid : nodes) {
            nsb.append(nid).append(",");
        }
        nsb.append("]\n");
        nsb.append("[edge: ");
        for (long eid : edges) {
            nsb.append(eid).append(",");
        }
        nsb.append("]");
        return nsb.toString();
    }

    public boolean hasShareNodeWithChain(Chain chain) {
        for (long nid : chain.nodeList) {
            if (nodes.contains(nid)) {
                return true;
            }
        }
        return false;
    }

    public boolean hasShareNodeWithComponent(ChainComponet dest_cp) {
        for (long nid : dest_cp.nodes) {
            if (nodes.contains(nid)) {
                return true;
            }
        }
        return false;
    }

    public void merge(ChainComponet dest_cp) {
        for (long nid : dest_cp.nodes) {
            addNode(nid);
        }

        for (long nid : dest_cp.edges) {
            addEdge(nid);
        }
    }
}
