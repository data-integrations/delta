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

package io.cdap.delta.app;

import com.google.gson.Gson;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.common.ContentProvider;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.delta.app.service.AssessmentService;
import io.cdap.delta.proto.PipelineReplicationState;
import io.cdap.delta.proto.StateRequest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 *  This Test class enables the SQL DB based state store
 *  and runs all the tests from DeltaPipelineStateStoreBaseTest
 */
public class PipelineDBStoreTest extends DeltaPipelineStateStoreBaseTest {

  private static URI serviceURI;
  private static final Gson GSON = new Gson();

  @BeforeClass
  public static void setupTest() throws Exception {
    setUpSystemServices(DeltaApp.class, "cdc_db");
    setupArtifacts(DeltaApp.class);

    ApplicationId pipeline = NamespaceId.SYSTEM.app("delta");
    ApplicationManager appManager = getApplicationManager(pipeline);
    ServiceManager serviceManager = appManager.getServiceManager(AssessmentService.NAME);
    serviceURI = serviceManager.getServiceURL().toURI();
  }

  @AfterClass
  public static void teardown() throws Exception {
    removeArtifacts("cdc_db");
  }

  @Override
  Long getMaxGenerationNum(ApplicationId appId, String path) throws IOException {
    Long maxGeneration = Long.parseLong(executeCall(HttpMethod.GET,
                                                    String.format("v1/contexts/%s/apps/%s/maxgeneration",
                                                                  appId.getNamespace(), appId.getApplication()),
                                                    null));
    return maxGeneration > 0 ? maxGeneration : null;
  }

  @Override
  OffsetAndSequence getOffset(DeltaWorkerId id, String path) throws IOException {
    String url = String.format("v1/contexts/%s/apps/%s/generations/%s/instances/%s/offset",
                               id.getPipelineId().getNamespace(), id.getPipelineId().getApp(),
                               id.getPipelineId().getGeneration(), id.getInstanceId());
    String offsetAndSequenceJsonStr = executeCall(HttpMethod.GET, url, null);
    if (offsetAndSequenceJsonStr.equals("")) {
      return null;
    }
    return GSON.fromJson(offsetAndSequenceJsonStr, OffsetAndSequence.class);
  }

  @Override
  PipelineReplicationState getPipelineReplicationState(DeltaWorkerId id, String path) throws IOException {
    StateRequest stateRequest = new StateRequest(id.getPipelineId().getApp(), "test_offsetbasepath");

    String offsetAndSequenceJsonStr = executeCall(
      HttpMethod.POST,
      String.format("v1/contexts/%s/getState", id.getPipelineId().getNamespace()),
      GSON.toJson(stateRequest));

    return GSON.fromJson(offsetAndSequenceJsonStr, PipelineReplicationState.class);
  }

  @Override
  String getOffsetBasePath() {
    return null;
  }

  private String executeCall(HttpMethod method, String methodUrl, String body) throws IOException {
    URL validatePipelineURL = serviceURI
      .resolve(methodUrl)
      .toURL();

    ContentProvider<InputStream> content = null;
    if (body != null) {
      content = () -> new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
    }

    HttpRequest request =
      new HttpRequest(method, validatePipelineURL, null, content, null);
    HttpResponse response = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));

    return response.getResponseBodyAsString();
  }

}
