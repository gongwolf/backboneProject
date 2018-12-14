package v3;

import org.neo4j.graphdb.Relationship;

public class RelationshipExt {
    boolean visited;
    Relationship relationship;
    int level;
    int start_id;
    int end_id;

    public RelationshipExt(Relationship relationship, int src_id, int dest_id) {
        this.relationship = relationship;
        this.level = 0;
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
}
