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

package io.cdap.delta.api;

import io.cdap.delta.api.assessment.StandardizedTableDetail;
import io.cdap.delta.api.assessment.TableAssessorSupplier;

/**
 * A CDC target, responsible for writing data to a storage system.
 */
public interface DeltaTarget extends TableAssessorSupplier<StandardizedTableDetail> {
  String PLUGIN_TYPE = "cdcTarget";

  /**
   * Configure the source. This is called when the application is deployed.
   *
   * @param configurer configurer used to set configuration settings
   */
  void configure(Configurer configurer);

  /**
   * Initialize the Delta Target stage. This is called after the application is deployed, whenever
   * the program is initialized.
   *
   * @param context {@link DeltaTargetContext}
   * @throws Exception if there is any error during initialization
   */
  default void initialize(DeltaTargetContext context) throws Exception {
  }

  /**
   * Create an event consumer that applies change events to the target system.
   *
   * @param context target context that provides access to application information and offset persistence
   * @return an event consumer that applies change events to the target system
   * @throws Exception if the consumer could not be created, which will result in the program failure
   */
  EventConsumer createConsumer(DeltaTargetContext context) throws Exception;
}
