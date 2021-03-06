package DataStructure;

import org.neo4j.graphdb.Relationship;

public class RelationshipExt {
    public long rel_id;
    public boolean visited;
    public Relationship relationship;
    public int level;
    public long start_id;
    public long end_id;

    public RelationshipExt(){
    }

    public RelationshipExt(Relationship relationship, long src_id, long dest_id) {
        this.rel_id = relationship.getId();
        this.relationship = relationship;
        this.visited = false;
        this.start_id = src_id;
        this.end_id = dest_id;
    }

    public RelationshipExt(RelationshipExt r) {
        this.relationship = r.relationship;
        this.level = 0;
        this.visited = r.visited;
    }

    public int getOtherId(int from_id){
        return (int) this.relationship.getOtherNodeId(from_id);
    }

    @Override
    public String toString() {
        return "RelationshipExt{" +
                "visited=" + visited +
                ", relationship=" + relationship +
                ", level=" + level +
                ", start_id=" + start_id +
                ", end_id=" + end_id +
                '}';
    }
}
