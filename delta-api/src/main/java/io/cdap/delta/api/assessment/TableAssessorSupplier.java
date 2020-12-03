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

import io.cdap.delta.api.Configurer;

/**
 * Creates TableAssessors
 *
 * @param <T> type of schema the assessor examines.
 */
public interface TableAssessorSupplier<T> {

  /**
   * Create a table assessor that will check if there will be potential problems replicating a table.
   *
   * @param configurer configurer used to instantiate plugins
   * @return a table assessor
   * @throws Exception if the table assessor could not be created
   */
  default TableAssessor<T> createTableAssessor(Configurer configurer) throws Exception {
   throw new UnsupportedOperationException();
  }

  /**
   * Create a table assessor that will check if there will be potential problems replicating a table.
   *
   * @param configurer configurer used to instantiate plugins
   * @param draftId the id of the replicator draft, it can be used as the prefix of the id for some temporary resources
   *                created by Assessor. With such prefix, it will be easy to detect/cleanup dangling resources
   * @return a table assessor
   * @throws Exception if the table assessor could not be created
   */
  default TableAssessor<T> createTableAssessor(Configurer configurer, String draftId) throws Exception {
    return createTableAssessor(configurer);
  }
}
