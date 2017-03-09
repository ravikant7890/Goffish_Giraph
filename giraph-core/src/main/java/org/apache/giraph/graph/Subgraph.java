package org.apache.giraph.graph;

import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Created by anirudh on 23/10/16.
 */
public interface Subgraph<S extends WritableComparable,
    I extends WritableComparable, V extends Writable,
    E extends Writable, SV extends Writable, EI extends WritableComparable> {

  SubgraphVertices<S, I, V, E, SV, EI> getSubgraphVertices();

  SubgraphVertex<S, I, V, E, EI> getVertexById(I vertexId);

  S getSubgraphId();

  long getVertexCount();

  long getLocalVertexCount();

  Iterable<SubgraphVertex<S, I, V, E, EI>> getVertices();

  Iterable<SubgraphVertex<S, I, V, E, EI>> getLocalVertices();

  Iterable<RemoteSubgraphVertex<S, I, V, E, EI>> getRemoteVertices();

  // TODO: Change function in API
  Iterable<SubgraphEdge<I, E, EI>> getVertexEdges();

  SubgraphEdge<I, E, EI> getEdgeById(EI edgeId);

  // TODO: Change function in API
  void setSubgraphValue(SV value);

  SV getSubgraphValue();

}
