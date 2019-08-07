package v3;

import DataStructure.TNode;
import org.neo4j.graphdb.Node;

public class GraphNode {
    TNode<RelationshipExt> frist = null;
    TNode<RelationshipExt> last = null;
    Node node;
}
