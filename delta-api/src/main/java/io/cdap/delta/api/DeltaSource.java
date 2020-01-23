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

import io.cdap.delta.api.assessment.TableRegistry;

/**
 * Pluggable interface for reading change events
 */
public interface DeltaSource {
  String PLUGIN_TYPE = "cdcSource";

  /**
   * Configure the source. This is called when the application is deployed.
   *
   * @param configurer configurer used to set configuration settings
   */
  void configure(Configurer configurer);

  /**
   * Create an event reader used to read change events. This is called after the application is deployed, whenever
   * the program is started.
   *
   * @param context program context
   * @return an event reader used to read change events
   */
  EventReader createReader(DeltaSourceContext context, EventEmitter eventEmitter);

  /**
   * Create a table registry that is used to fetch information about tables in databases.
   *
   * @param context program context
   * @return table registry used to fetch information about tables in databases.
   */
  TableRegistry createTableRegistry(DeltaRuntimeContext context);
}
