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
package org.apache.giraph.conf;

import static org.apache.giraph.conf.GiraphConstants.*;
import static org.apache.giraph.utils.ConfigurationUtils.getTypesHolderClass;
import static org.apache.giraph.utils.ReflectionUtils.getTypeArguments;

import org.apache.giraph.graph.DefaultVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Preconditions;

/**
 * Holder for the generic types that describe user's graph.
 *
 * @param <I> Vertex ID class
 * @param <V> Vertex Value class
 * @param <E> Edge class
 */
public class GiraphTypes<I extends WritableComparable, V extends Writable,
    E extends Writable> {
  /** Vertex id class */
  private Class<I> vertexIdClass;
  /** Vertex value class */
  private Class<V> vertexValueClass;
  /** Edge value class */
  private Class<E> edgeValueClass;
  /** Outgoing message value class */
  private Class<? extends Writable> outgoingMessageValueClass;
  /** Vertex implementation class */
  private Class<? extends Vertex> vertexClass = DefaultVertex.class;

  private Class<? extends WritableComparable> edgeIdClass;

  private Class<? extends Writable> subgraphValueClass;
  private Class<? extends Writable> subgraphMessageValueClass;

  private Class<? extends Writable> subgraphVertexValueClass;

  private Class<? extends WritableComparable> subgraphIdClass;

  private Class<? extends WritableComparable> subgraphVertexIdClass;

  /**
   * Empty Constructor
   */
  public GiraphTypes() { }

  /**
   * Constructor taking values
   *
   * @param vertexIdClass vertex id class
   * @param vertexValueClass vertex value class
   * @param edgeValueClass edge value class
   * @param incomingMessageValueClass incoming message class
   * @param outgoingMessageValueClass outgoing message class
   */
  public GiraphTypes(Class<I> vertexIdClass,
      Class<V> vertexValueClass,
      Class<E> edgeValueClass,
      Class<? extends Writable> incomingMessageValueClass,
      Class<? extends Writable> outgoingMessageValueClass) {
    this.edgeValueClass = edgeValueClass;
    this.outgoingMessageValueClass = outgoingMessageValueClass;
    this.vertexIdClass = vertexIdClass;
    this.vertexValueClass = vertexValueClass;
  }

  /**
   * Read types from a {@link Configuration}.
   * First tries to read them directly from the configuration options.
   * If that doesn't work, tries to infer from {@link TypesHolder}.
   *
   * @param conf Configuration
   * @param <IX> vertex id
   * @param <VX> vertex value
   * @param <EX> edge value
   * @return GiraphTypes
   */
  public static <IX extends WritableComparable, VX extends Writable,
        EX extends Writable> GiraphTypes<IX, VX, EX> readFrom(
      Configuration conf) {
    GiraphTypes<IX, VX, EX> types = new GiraphTypes<IX, VX, EX>();
    types.readDirect(conf);
    if (!types.hasData()) {
      Class<? extends TypesHolder> klass = getTypesHolderClass(conf);
      if (klass != null) {
        types.inferFrom(klass);
      }
    }
    return types;
  }

  /**
   * Infer types from Computation class
   *
   * @param klass Computation class
   */
  public void inferFrom(Class<? extends TypesHolder> klass) {
    Class<?>[] classList = getTypeArguments(TypesHolder.class, klass);
    Preconditions.checkArgument(classList.length == 5);
    vertexIdClass = (Class<I>) classList[0];
    vertexValueClass = (Class<V>) classList[1];
    edgeValueClass = (Class<E>) classList[2];
    outgoingMessageValueClass = (Class<? extends Writable>) classList[4];
  }

  /**
   * Read types directly from Configuration
   *
   * @param conf Configuration
   */
  private void readDirect(Configuration conf) {
    vertexIdClass = (Class<I>) VERTEX_ID_CLASS.get(conf);
    vertexValueClass = (Class<V>) VERTEX_VALUE_CLASS.get(conf);
    edgeValueClass = (Class<E>) EDGE_VALUE_CLASS.get(conf);
    outgoingMessageValueClass = OUTGOING_MESSAGE_VALUE_CLASS.get(conf);
    vertexClass = VERTEX_CLASS.get(conf);
    edgeIdClass = EDGE_ID_CLASS.get(conf);
    subgraphValueClass = SUBGRAPH_VALUE_CLASS.get(conf);
    subgraphVertexValueClass = SUBGRAPH_VERTEX_VALUE_CLASS.get(conf);
    subgraphIdClass = SUBGRAPH_ID_CLASS.get(conf);
    subgraphVertexIdClass = SUBGRAPH_VERTEX_ID_CLASS.get(conf);
    subgraphMessageValueClass = SUBGRAPH_MESSAGE_VALUE_CLASS.get(conf);
  }

  /**
   * Check if types are set
   *
   * @return true if types are set
   */
  public boolean hasData() {
    return vertexIdClass != null &&
        vertexValueClass != null &&
        edgeValueClass != null &&
        outgoingMessageValueClass != null;
  }

  /**
   * Write types to Configuration
   *
   * @param conf Configuration
   */
  public void writeTo(Configuration conf) {
    VERTEX_ID_CLASS.set(conf, vertexIdClass);
    VERTEX_VALUE_CLASS.set(conf, vertexValueClass);
    EDGE_VALUE_CLASS.set(conf, edgeValueClass);
    OUTGOING_MESSAGE_VALUE_CLASS.set(conf, outgoingMessageValueClass);
  }

  /**
   * Write types to Configuration if not already set
   *
   * @param conf Configuration
   */
  public void writeIfUnset(Configuration conf) {
    VERTEX_ID_CLASS.setIfUnset(conf, vertexIdClass);
    VERTEX_VALUE_CLASS.setIfUnset(conf, vertexValueClass);
    EDGE_VALUE_CLASS.setIfUnset(conf, edgeValueClass);
    OUTGOING_MESSAGE_VALUE_CLASS.setIfUnset(conf, outgoingMessageValueClass);
  }

  public Class<E> getEdgeValueClass() {
    return edgeValueClass;
  }

  Class<? extends Writable> getInitialOutgoingMessageValueClass() {
    return outgoingMessageValueClass;
  }

  public Class<I> getVertexIdClass() {
    return vertexIdClass;
  }

  public Class<V> getVertexValueClass() {
    return vertexValueClass;
  }

  public Class<? extends Vertex> getVertexClass() {
    return vertexClass;
  }

  public Class<? extends WritableComparable> getSubgraphIdClass() {
    return subgraphIdClass;
  }

  public Class<? extends WritableComparable> getSubgraphVertexIdClass() {
    return subgraphVertexIdClass;
  }


  public Class<? extends WritableComparable> getEdgeIdClass() {
    return edgeIdClass;
  }

  public Class<? extends Writable> getSubgraphValueClass() {
    return subgraphValueClass;
  }

  public Class<? extends Writable> getSubgraphMessageValueClass() {
    return subgraphMessageValueClass;
  }

  public Class<? extends Writable> getSubgraphVertexValueClass() {
    return subgraphVertexValueClass;
  }

  public void setEdgeValueClass(Class<E> edgeValueClass) {
    this.edgeValueClass = edgeValueClass;
  }

  public void setVertexIdClass(Class<I> vertexIdClass) {
    this.vertexIdClass = vertexIdClass;
  }

  public void setVertexValueClass(Class<V> vertexValueClass) {
    this.vertexValueClass = vertexValueClass;
  }

  public void setOutgoingMessageValueClass(
      Class<? extends Writable> outgoingMessageValueClass) {
    this.outgoingMessageValueClass = outgoingMessageValueClass;
  }
}
