/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.delta.app.service.remote;

import io.cdap.delta.proto.DeltaConfig;

import javax.annotation.Nullable;

/**
 * Request class for remote assessment tasks
 */
public class RemoteAssessmentRequest {

  private String namespace;
  private DeltaConfig config;
  private String request;

  public RemoteAssessmentRequest(String namespace, DeltaConfig config, @Nullable String request) {
    this.namespace = namespace;
    this.config = config;
    this.request = request;
  }

  public String getNamespace() {
    return namespace;
  }

  public DeltaConfig getConfig() {
    return config;
  }

  @Nullable
  public String getRequest() {
    return request;
  }
}
