package Index;

import Neo4jTools.Neo4jDB;

import java.util.ArrayList;

public class ChainComponents {
    private final Neo4jDB neo4jObj;
    ArrayList<ChainComponet> c_components = new ArrayList<>();
    private int numComps;

    public ChainComponents(Neo4jDB neo4jObj) {
        this.neo4jObj = neo4jObj;
    }

    public void add(Chain chain) {
        int ccp_idx = hasShareNodeWithExistedComponent(chain);
        ChainComponet ccp;
        if (ccp_idx == -1) {
            ccp = new ChainComponet();
            this.c_components.add(ccp);
            numComps++;
        } else {
            ccp = c_components.get(ccp_idx);
        }

        for (int i = 0; i < chain.length; i++) {
            long nodeid = -1, next_node_id = -1, relid = -1;
            if (i != (chain.length - 1)) {
                nodeid = chain.nodeList.get(i);
                next_node_id = chain.nodeList.get(i + 1);
            } else if (chain.isCycle) {
                nodeid = chain.nodeList.get(i);
                next_node_id = chain.nodeList.get(0);
            }
            if (nodeid != -1 && next_node_id != -1) {
                relid = neo4jObj.getRelationShipByStartAndEndNodeID(nodeid, next_node_id);
                ccp.addNode(nodeid);
                ccp.addEdge(relid);
            }
        }
    }

    /**
     * If there is no component or no component share any node with chain return -1,
     * otherwise, return the index of the component that shared the node with given chain
     */

    private int hasShareNodeWithExistedComponent(Chain chain) {
        for (int i = 0; i < this.c_components.size(); i++) {
            ChainComponet ccp = c_components.get(i);
            if (ccp.hasShareNodeWithChain(chain)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (ChainComponet ccp : c_components) {
            sb.append(ccp.toString()).append("\n");
        }
        return sb.toString().trim();
    }

    public void mergeComponents() {
        int current_num_cp = this.numComps;
        boolean flag = false;
        do {
            for (int i = 0; i < this.c_components.size(); i++) {
                ChainComponet source_cp = this.c_components.get(i);

                for (int j = i + 1; j < this.c_components.size(); ) {
                    ChainComponet dest_cp = this.c_components.get(j);
                    if (source_cp.hasShareNodeWithComponent(dest_cp)) {
                        source_cp.merge(dest_cp);
                        c_components.remove(j);
                    } else {
                        j++;
                    }
                }
            }

            this.numComps = c_components.size();
            flag = numComps != current_num_cp; // if merge happened
            current_num_cp = numComps;
//            System.out.println(this);
//            System.out.println("------------");
        } while (flag);
    }
}
