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
package org.apache.giraph.function.primitive.pairs;

import java.io.Serializable;

${generated_message}

/**
 * Primitive specialization of Function:
 * (${type1.lower}, ${type2.lower}) -> boolean
 */
public interface ${type1.camel}${type2.camel}Predicate extends Serializable {
  /**
   * Returns the result of applying this predicate to {@code input}.
   *
   * @param input1 First input
   * @param input2 Second input
   * @return result
   */
  boolean apply(${type1.lower} input1, ${type2.lower} input2);
}
