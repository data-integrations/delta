/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.delta.store;

import com.google.gson.Gson;
import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.delta.api.DeltaPipelineId;
import io.cdap.delta.app.DeltaWorkerId;
import io.cdap.delta.app.OffsetAndSequence;

import java.util.Collection;
import javax.ws.rs.HttpMethod;

/**
 * Class to handle Rest Api calls to System Service [ Assessor ]
 */

public class RemoteStateStore implements StateStore {

  private static final String BASE_PATH = "v1";
  protected static final String OFFSET_KEY = "offset";

  private AssessmentServiceClient assessmentServiceClient;
  private static final Gson GSON = new Gson();

  public RemoteStateStore(ServiceDiscoverer context) {
    this(new AssessmentServiceClient(context));
  }

  public RemoteStateStore(AssessmentServiceClient assessmentServiceClient) {
    this.assessmentServiceClient = assessmentServiceClient;
  }

  @Override
  public OffsetAndSequence readOffset(DeltaWorkerId id) {
    return assessmentServiceClient
      .retryableApiCall(getMethodPath(id, OFFSET_KEY), HttpMethod.GET, OffsetAndSequence.class);
  }

  @Override
  public void writeOffset(DeltaWorkerId id, OffsetAndSequence offset) {
    String requestBody = GSON.toJson(offset);
    assessmentServiceClient.retryableApiCall(getMethodPath(id, OFFSET_KEY), HttpMethod.PUT, requestBody);
  }

  @Override
  public byte[] readState(DeltaWorkerId id, String key) {
    return assessmentServiceClient
      .retryableApiCall(getMethodPathState(id, key), HttpMethod.GET);
  }

  @Override
  public void writeState(DeltaWorkerId id, String key, byte[] val) {
    assessmentServiceClient.retryableApiCall(getMethodPathState(id, key), HttpMethod.PUT, val);
  }

  @Override
  public Long getLatestGeneration(String namespace, String pipelineName) {
    throw new UnsupportedOperationException("getLatestGeneration is not supposed to be called from a worker instance");
  }

  @Override
  public Collection<Integer> getWorkerInstances(DeltaPipelineId pipelineId) {
    throw new UnsupportedOperationException("getWorkerInstances is not supposed to be called from a worker instance");
  }

  private String getMethodPathState(DeltaWorkerId id, String state) {
    return getMethodPath(id, String.format("states/%s", state));
  }

  private String getMethodPath (DeltaWorkerId id, String lastResourcePath) {
    return String.format("%s/contexts/%s/apps/%s/generations/%d/instances/%d/%s", BASE_PATH,
                  id.getPipelineId().getNamespace(), id.getPipelineId().getApp(),
                  id.getPipelineId().getGeneration(), id.getInstanceId(), lastResourcePath);
  }
}
