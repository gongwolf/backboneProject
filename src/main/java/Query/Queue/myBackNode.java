package Query.Queue;

import Query.backbonePath;

import java.util.ArrayList;

public class myBackNode {
    public boolean inqueue;
    public ArrayList<backbonePath> skypaths = new ArrayList<>();
    public long id;
    public long source_id;

    public myBackNode(long source_node) {
        id = source_id = source_node;
        backbonePath sourceDummyResult = new backbonePath(source_node);
        skypaths.add(sourceDummyResult);
    }

    public myBackNode(backbonePath n_bp) {
        this.id = n_bp.destination;
        this.source_id = n_bp.source;
    }


    public boolean addtoSkyline(backbonePath new_bp) {
        int i = 0;

        if (this.skypaths.isEmpty()) {
            this.skypaths.add(new_bp);
            return true;
        } else {
            boolean can_insert_np = true;
            for (; i < this.skypaths.size(); ) {
                if (checkDominated(this.skypaths.get(i).costs, new_bp.costs)) {
                    can_insert_np = false;
                    break;
                } else {
                    if (checkDominated(new_bp.costs, this.skypaths.get(i).costs)) {
                        this.skypaths.remove(i);
                    } else {
                        i++;
                    }
                }
            }
            if (can_insert_np) {
                this.skypaths.add(new_bp);
                return true;
            }
        }
        return false;
    }

    /**
     * if all the costs of the target path is less than the estimated costs of the wanted path, means target path dominate the wanted path
     *
     * @param costs          the target path
     * @param estimatedCosts the wanted path
     * @return if the target path dominates the wanted path, return true. other wise return false.
     */
    private boolean checkDominated(double[] costs, double[] estimatedCosts) {
        for (int i = 0; i < costs.length; i++) {
            if (costs[i] * (1) > estimatedCosts[i]) {
                return false;
            }
        }
        return true;
    }
}
