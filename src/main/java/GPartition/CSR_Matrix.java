package GPartition;

public class CSR_Matrix {
    long[] A;
    long[] indices ;
    long[] index_col;

    public CSR_Matrix(String graph_path){

    }

    public CSR_Matrix() {

    }


    @Override
    protected Object clone() throws CloneNotSupportedException {
        CSR_Matrix n_csr = new CSR_Matrix();
        n_csr.A = new long[this.A.length];
        n_csr.indices = new long[this.indices.length];
        n_csr.index_col = new long[this.index_col.length];

        System.arraycopy(this.A,0,n_csr.A,0,this.A.length);
        System.arraycopy(this.indices,0,n_csr.indices,0,this.indices.length);
        System.arraycopy(this.index_col,0,n_csr.index_col,0,this.index_col.length);

        return (Object) n_csr;
    }
}
