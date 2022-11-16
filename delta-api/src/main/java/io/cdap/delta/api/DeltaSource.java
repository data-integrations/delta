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

wdwdw
package io.cdap.delta.api;

import io.cdap.delta.api.assessment.TableAssessorSupplier;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableRegistry;

/**
 * Pluggable interface for reading change events
 */
public interface DeltaSource extends TableAssessorSupplier<TableDetail> {
  String PLUGIN_TYPE = "cdcSource";

  /**
   * Configure the source. This is called when the application is deployed.
   *
   * @param configurer source configurer used to set configuration settings and register plugins
   */
  void configure(SourceConfigurer configurer);

  /**
   * Initialize the Delta Source stage. This is called after the application is deployed, whenever
   * the program is initialized.
   *
   * @param context {@link DeltaSourceContext}
   * @throws Exception if there is any error during initialization
   */
  default void initialize(DeltaSourceContext context) throws Exception {
  }

  /**
   * Create an event reader used to read change events. This is called after the application is deployed, whenever
   * the program is started. If an exception is thrown, the pipeline run will fail.
   *
   * @param definition defines what type of information the reader should read
   * @param context program context
   * @param eventEmitter emits events that need to be replicated
   * @return an event reader used to read change events
   * @throws Exception if there was an error creating the event reader
   */
  EventReader createReader(EventReaderDefinition definition, DeltaSourceContext context,
                           EventEmitter eventEmitter) throws Exception;

  /**
   * Create a table registry that is used to fetch information about tables in databases.
   * This is called when the pipeline is being configured, before it is deployed.
   *
   * @param configurer configurer used to instantiate plugins
   * @return table registry used to fetch information about tables in databases.
   * @throws Exception if there was an error creating the table registry
   */
  TableRegistry createTableRegistry(Configurer configurer) throws Exception;
}
