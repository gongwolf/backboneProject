package GPartition;

import java.util.HashSet;

public class Graph {
    public long number_of_nodes;
    CSR_Matrix csr_matrix;


    public Graph() {
        this.number_of_nodes=0;
        this.csr_matrix = new CSR_Matrix();
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        Graph ngraph = new Graph();
        ngraph.number_of_nodes = this.number_of_nodes;
        ngraph.csr_matrix = (CSR_Matrix) this.csr_matrix.clone();
        return super.clone();
    }
}
