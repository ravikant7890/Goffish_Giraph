package org.apache.giraph.graph;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.python.antlr.op.Sub;

/**
 * Created by anirudh on 02/11/16.
 */
public interface RemoteSubgraphVertex<S extends WritableComparable,
    I extends WritableComparable, V extends Writable, E extends Writable, EI extends WritableComparable>
    extends SubgraphVertex<S, I, V, E, EI> {
  SubgraphId<S> getSubgraphId();

}
