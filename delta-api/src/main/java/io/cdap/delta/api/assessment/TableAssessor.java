/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.delta.api.assessment;

import java.io.Closeable;
import java.util.Collections;

/**
 * Creates assessments, highlighting potential problems. This is used when a pipeline is being created to give
 * users early feedback on configuration or environmental issues.
 *
 * @param <T> type of table descriptor to assess
 */
public interface TableAssessor<T> extends Closeable {

  /**
   * Assess whether there will be potential problems replicating data from the specified table.
   *
   * @param tableDescriptor descriptor about the table to replicate
   * @return assessment of potential problems
   */
  TableAssessment assess(T tableDescriptor);

  /**
   * Assess whether there will be any general potential problems replicating data based unrelated to a specific table.
   * For example, if there are problems connecting to the source/target system.
   *
   * @return an assessment of potential problems
   */
  default Assessment assess() {
    return new Assessment(Collections.emptyList(), Collections.emptyList());
  }

  default void close() {
    // no-op
  }
}
