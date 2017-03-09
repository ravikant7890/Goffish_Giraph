package org.apache.giraph.graph;

import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by anirudh on 18/10/16.
 */
public interface SubgraphEdge<I extends WritableComparable, E extends Writable, EI extends WritableComparable> extends Writable {

    I getSinkVertexId();

    E getValue();

    EI getEdgeId();

    void setValue(E value);


}
