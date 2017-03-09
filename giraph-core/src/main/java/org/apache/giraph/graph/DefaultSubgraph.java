package org.apache.giraph.graph;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by anirudh on 27/09/16.
 *
 * @param <S>  Subgraph id
 * @param <I>  Vertex id
 * @param <V>  Vertex value
 * @param <E>  Edge data
 * @param <SV> Subgraph Value type
 */
public class DefaultSubgraph<S extends WritableComparable,
    I extends WritableComparable, V extends Writable, E extends Writable, SV extends Writable, EI extends WritableComparable>
    extends DefaultVertex<SubgraphId<S>, SubgraphVertices<S, I, V, E, SV, EI>, E> implements Subgraph<S, I, V, E, SV, EI> {

  public void setRemoteVertices(HashMap<I, RemoteSubgraphVertex<S, I, V, E, EI>> remoteVertices) {
    SubgraphVertices<S, I, V, E, SV, EI> subgraphVertices = getValue();
    subgraphVertices.setRemoteVertices(remoteVertices);
  }

  public Iterable<RemoteSubgraphVertex<S, I, V, E, EI>> getRemoteVertices() {
    SubgraphVertices<S, I, V, E, SV, EI> subgraphVertices = getValue();
    return subgraphVertices.getRemoteVertices().values();
  }

  @Override
  public SubgraphEdge<I, E, EI> getEdgeById(EI edgeId) {
    return null;
  }

  @Override
  public void setSubgraphValue(SV value) {
    getSubgraphVertices().setSubgraphValue(value);
  }

  @Override
  public SV getSubgraphValue() {
    return getSubgraphVertices().getSubgraphValue();
  }

  @Override
  public SubgraphVertices<S, I, V, E, SV, EI> getSubgraphVertices() {
    return getValue();
  }

  @Override
  public SubgraphVertex<S, I, V, E, EI> getVertexById(I vertexId) {
    return null;
  }

  @Override
  public S getSubgraphId() {
    return getId().getSubgraphId();
  }

  @Override
  public long getVertexCount() {
    return getSubgraphVertices().getNumVertices() + getSubgraphVertices().getNumRemoteVertices();
  }

  @Override
  public long getLocalVertexCount() {
    return getSubgraphVertices().getNumVertices();
  }

  @Override
  public Iterable<SubgraphVertex<S, I, V, E, EI>> getVertices() {
    return getSubgraphVertices().getVertices();
  }

  @Override
  public Iterable<SubgraphVertex<S, I, V, E, EI>> getLocalVertices() {
    return getSubgraphVertices().getLocalVertices().values();
  }

  public int getPartitionId() {
    return getId().getPartitionId();
  }

  public Iterable<SubgraphEdge<I, E, EI>> getVertexEdges() {
    return new Iterable<SubgraphEdge<I, E, EI>>() {
      @Override
      public Iterator<SubgraphEdge<I, E, EI>> iterator() {
        return new EdgeIterator();
      }
    };
  }

  private final class EdgeIterator implements Iterator<SubgraphEdge<I, E, EI>> {
    Iterator<SubgraphVertex<S, I, V, E, EI>> vertexMapIterator;
    Iterator<SubgraphEdge<I, E, EI>> edgeIterator;

    public EdgeIterator() {
      vertexMapIterator = getVertices().iterator();
      SubgraphVertex<S, I, V, E, EI> nextVertex = vertexMapIterator.next();
      edgeIterator = nextVertex.getOutEdges().iterator();
    }

    @Override
    public boolean hasNext() {
      if (edgeIterator.hasNext()) {
        return true;
      } else {
        while (vertexMapIterator.hasNext()) {
          SubgraphVertex<S, I, V, E, EI> nextVertex = vertexMapIterator.next();
          edgeIterator = nextVertex.getOutEdges().iterator();
          if (edgeIterator.hasNext()) {
            return true;
          }
        }
      }
      return false;
    }

    public SubgraphEdge<I, E, EI> next() {
      if (edgeIterator.hasNext()) {
        return edgeIterator.next();
      } else {
        while (vertexMapIterator.hasNext()) {
          SubgraphVertex<S, I, V, E, EI> nextVertex = vertexMapIterator.next();
          edgeIterator = nextVertex.getOutEdges().iterator();
          if (edgeIterator.hasNext()) {
            return edgeIterator.next();
          }
        }
      }
      return null;
    }

    // TODO: Raise exception on call to remove
    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }



}