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

import io.cdap.cdap.api.metrics.Metrics;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Runtime context for a CDC component.
 */
public interface DeltaRuntimeContext {

  /**
   * @return the application name
   */
  String getApplicationName();

  /**
   * @return the program run id
   */
  String getRunId();

  /**
   * @return metrics emitter
   */
  Metrics getMetrics();

  /**
   * Get state associated with the given key.
   *
   * @param key key to fetch state with
   * @return state associated with the given key
   */
  @Nullable
  byte[] getState(String key) throws IOException;

  /**
   * Write state for a given key. Any existing state will be overwritten.
   *
   * @param key key to write to
   * @param val state to write
   */
  void putState(String key, byte[] val) throws IOException;
}
