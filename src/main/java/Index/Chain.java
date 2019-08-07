package Index;

import java.util.ArrayList;

public class Chain {
    public int length;
    public boolean isCycle;
    public int index;
    public ArrayList<Long> nodeList;

    public Chain() {
        nodeList = new ArrayList<>();
        length = nodeList.size();
        isCycle = false;
    }

    public Chain(ArrayList<Long> nlist) {
        nodeList = new ArrayList<>();
        nodeList.addAll(nlist);
        this.length = nodeList.size();
    }

    public Chain(Chain c) {
        nodeList = new ArrayList<>();
        nodeList.addAll(c.nodeList);
        this.length = nodeList.size();
        this.isCycle = c.isCycle;
    }

    public static void main(String args[]) {
        ArrayList<Long> a1 = new ArrayList<>();
        a1.add(402L);
        a1.add(403L);
        a1.add(404L);

        ArrayList<Long> a2 = new ArrayList<>();
        a2.add(940L);
        a2.add(402L);
        a2.add(403L);
        a2.add(404L);

        Chain c1 = new Chain(a1);
        Chain c2 = new Chain(a2);

        System.out.println(c1.isPartOfChain(c2) + "  " + c1.equals(c2));

        String aaaa = "aaaa\naaaa\n";
        System.out.println(aaaa);
        System.out.println("-----------------");
        String bbbb = aaaa.trim();
        System.out.println(bbbb);
        System.out.println("-----------------");
    }

    public void add(long nodeID) {
        this.nodeList.add(nodeID);
        this.length++;
    }

    public boolean containsRelationShip(long startNodeId, long endNodeId) {
        if (length == 0) {
            return false;
        }

        for (int i = 0; i < length - 1; i++) {
            if (nodeList.get(i) == startNodeId && (nodeList.get(i + 1) == endNodeId)) {
                return true;
            }
        }

        if (nodeList.get(0) == startNodeId && nodeList.get(length - 1) == endNodeId) {
            return true;
        }

        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (long nid : this.nodeList) {
            sb.append(nid + "-");
        }
        String result = sb.substring(0, sb.length() - 1) + " id:"+this.index+"   isCycle:"+this.isCycle+"";
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        /* Check if o is an instance of Complex or not
          "null instanceof [type]" also returns false */
        if (!(obj instanceof Chain)) {
            return false;
        }

        // typecast o to Complex so that we can compare data members
        Chain c = (Chain) obj;

        // Compare the data members and return accordingly
        return c.nodeList.equals(this.nodeList);
    }

    /**
     * Check this chain is part of the given chain oc
     *
     * @param oc
     */
    public boolean isPartOfChain(Chain oc) {
//        System.out.println("length  :" + this.length + "     " + oc.length);
        if (oc.length < this.length) {
            return false;
        } else {
            for (int i = 0; i <= oc.length - this.length; i++) {
                long tmp_start_ov = oc.nodeList.get(i);
                long start_tv = this.nodeList.get(0);
//                System.out.println(tmp_start_ov+ "  " + start_tv + " " + (tmp_start_ov == start_tv));
                if (tmp_start_ov == start_tv) {
                    int j = 1;
                    for (; j < this.length; j++) {
                        long ov = oc.nodeList.get(i + j);
                        long tv = this.nodeList.get(j);
                        if (ov != tv) {
                            break;
                        }
                    }

                    if (j == this.length)
                        return true;
                }
//                System.out.println("---------------------------------");
            }
        }
        return false;
    }

    public void copyOf(Chain c) {
        this.nodeList.clear();
        this.nodeList.addAll(c.nodeList);
        this.length = c.length;
        this.isCycle = c.isCycle;
    }
}



