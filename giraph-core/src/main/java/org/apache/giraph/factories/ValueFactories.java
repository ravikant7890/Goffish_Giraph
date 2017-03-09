/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.factories;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import static org.apache.giraph.conf.GiraphConstants.*;

/**
 * Holder for factories to create user types.
 *
 * Note that we don't store the {@link MessageValueFactory} here because they
 * reference types which may change at a given superstep. Instead we create them
 * as necessary so that they get the latest information.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public class ValueFactories<I extends WritableComparable,
    V extends Writable, E extends Writable> {
  /** Vertex ID factory. */
  private final VertexIdFactory<I> vertexIdFactory;
  /** Vertex value factory. */
  private final VertexValueFactory<V> vertexValueFactory;
  /** Edge value factory. */
  private final EdgeValueFactory<E> edgeValueFactory;

  private final SubgraphValueFactory<? extends Writable> subgraphValueFactory;

  private final SubgraphVertexValueFactory<? extends Writable> subgraphVertexValueFactory;

  private final EdgeIdFactory<? extends WritableComparable> edgeIdFactory;

  private final SubgraphIdFactory<? extends WritableComparable> subgraphIdFactory;

  private final SubgraphVertexIdFactory<? extends WritableComparable> subgraphVertexIdFactory;

  private final SubgraphMessageValueFactory<? extends WritableComparable> subgraphMessageValueFactory;

  /**
   *
   * Constructor reading from Configuration
   *
   * @param conf Configuration to read from
   */
  public ValueFactories(Configuration conf) {
    vertexIdFactory = VERTEX_ID_FACTORY_CLASS.newInstance(conf);
    vertexValueFactory = VERTEX_VALUE_FACTORY_CLASS.newInstance(conf);
    edgeValueFactory = EDGE_VALUE_FACTORY_CLASS.newInstance(conf);
    subgraphValueFactory = SUBGRAPH_VALUE_FACTORY_CLASS.newInstance(conf);
    subgraphVertexValueFactory = SUBGRAPH_VERTEX_VALUE_FACTORY_CLASS.newInstance(conf);
    edgeIdFactory = EDGE_ID_FACTORY_CLASS.newInstance(conf);
    subgraphIdFactory = SUBGRAPH_ID_FACTORY_CLASS.newInstance(conf);
    subgraphVertexIdFactory = SUBGRAPH_VERTEX_ID_FACTORY_CLASS.newInstance(conf);
    subgraphMessageValueFactory = SUBGRAPH_MESSAGE_VALUE_FACTORY_CLASS.newInstance(conf);
  }

  public EdgeIdFactory<? extends WritableComparable> getEdgeIdFactory() {
    return edgeIdFactory;
  }

  public EdgeValueFactory<E> getEdgeValueFactory() {
    return edgeValueFactory;
  }

  public VertexIdFactory<I> getVertexIdFactory() {
    return vertexIdFactory;
  }

  public VertexValueFactory<V> getVertexValueFactory() {
    return vertexValueFactory;
  }

  public SubgraphValueFactory<? extends Writable> getSubgraphValueFactory() {
    return subgraphValueFactory;
  }

  public SubgraphVertexValueFactory<? extends Writable> getSubgraphVertexValueFactory() {
    return subgraphVertexValueFactory;
  }

  public SubgraphIdFactory<? extends WritableComparable> getSubgraphIdFactory() {
    return subgraphIdFactory;
  }

  public SubgraphVertexIdFactory<? extends WritableComparable> getSubgraphVertexIdFactory() {
    return subgraphVertexIdFactory;
  }

  public SubgraphMessageValueFactory<? extends Writable> getSubgraphMessageValueFactory() {
    return subgraphMessageValueFactory;
  }




}
