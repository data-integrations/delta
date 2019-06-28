/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.delta.app;

/**
 * Uniquely identifies a delta pipeline
 */
public class DeltaPipelineId {
  private final String namespace;
  private final String app;
  private final String generation;

  public DeltaPipelineId(String namespace, String app, String generation) {
    this.namespace = namespace;
    this.app = app;
    this.generation = generation;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getApp() {
    return app;
  }

  public String getGeneration() {
    return generation;
  }
}
