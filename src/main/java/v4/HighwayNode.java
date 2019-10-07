package v4;

import DataStructure.Monitor;

import java.util.ArrayList;

public class HighwayNode {
    long src_id;
    long highway_id;
    ArrayList<backbonePath> skylines = new ArrayList<>();

    public HighwayNode(long src) {
        this.src_id = src;
        this.highway_id = src;
        backbonePath bp = new backbonePath(src);
        skylines.add(bp);
    }

    public HighwayNode(long src, long highway_node_id) {
        src_id=src;
        highway_id = highway_node_id;
    }


    public boolean addToSkyline(backbonePath bp, Monitor monitor) {
        monitor.callAddToSkyline++;
        int i = 0;
        if (skylines.isEmpty()) {
            skylines.add(bp);
            return true;
        } else {
            boolean can_insert_np = true;
            for (; i < skylines.size(); ) {
                if (checkDominated(skylines.get(i).costs, bp.costs)) {
                    can_insert_np = false;
                    break;
                } else {
                    if (checkDominated(bp.costs, skylines.get(i).costs)) {
                        skylines.remove(i);
                    } else {
                        i++;
                    }
                }
            }
            if (can_insert_np) {
                skylines.add(bp);
                return true;
            }
        }
        return false;
    }

    /**
     * if costs dominated estimatedCosts, every value of cost is less or equal than estimatedCosts.
     * If costs is equal to estimatedCosts, then return false. So, the equal costs will not insert into the skyline paths.
     */
    private boolean checkDominated(double[] costs, double[] estimatedCosts) {

        int equalnum = 0;

        for (int i = 0; i < costs.length; i++) {
            if (costs[i] * (1) > estimatedCosts[i]) {
                return false;
            } else if (costs[i] * (1) == estimatedCosts[i]) {
                equalnum++;
            }
        }

        return true;

//        if (equalnum == costs.length) {
//            return false;
//        } else {
//            return true;
//        }
    }
}
