package org.apache.giraph.graph;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.LinkedList;

/**
 * Created by anirudh on 02/11/16.
 */
public class DefaultRemoteSubgraphVertex<S extends WritableComparable,
    I extends WritableComparable, V extends Writable, E extends Writable, EI extends WritableComparable>
    extends DefaultSubgraphVertex<S, I, V, E, EI> implements RemoteSubgraphVertex<S, I, V, E, EI> {

  private SubgraphId<S> subgraphId;

  public void initialize(SubgraphId<S> subgraphId, I vertexId, V value, LinkedList<SubgraphEdge<I, E, EI>> subgraphEdges) {
    this.subgraphId = subgraphId;
    super.initialize(vertexId, value, subgraphEdges);
  }

  @Override
  public boolean isRemote() {
    return true;
  }

  @Override
  public SubgraphId<S> getSubgraphId() {
    return subgraphId;
  }

  public void setSubgraphId(SubgraphId<S> subgraphId) {
    this.subgraphId = subgraphId;
  }

//  @Override
//  public V getValue() {
//    throw new UnsupportedOperationException("getValue() not supported for remote vertices");
//  }
}
