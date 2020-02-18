package v5LinkedList.clusterversion;

import org.jetbrains.annotations.NotNull;

public class NodeCoefficient implements Comparable {
    double coefficient;
    int first_hop_neighbors;
    int second_hop_neighbors;

    public NodeCoefficient(double coefficient, int first_hop_neighbors, int second_hop_neighbors) {
        this.coefficient = coefficient;
        this.first_hop_neighbors = first_hop_neighbors;
        this.second_hop_neighbors = second_hop_neighbors;
    }

    @Override
    public int compareTo(@NotNull Object o) {
        if (o == null) {
            return -1;
        } else {
            NodeCoefficient other_coff_obj = (NodeCoefficient) o;
            return (int) (this.coefficient - other_coff_obj.coefficient);
        }
    }

    public int getNumberOfTwoHopNeighbors() {
        return this.first_hop_neighbors + this.second_hop_neighbors;
    }
}
