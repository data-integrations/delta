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
import io.cdap.delta.api.Offset;
import io.cdap.delta.app.OffsetAndSequence;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.HttpMethod;

public class MockAssessmentServiceClient extends AssessmentServiceClient {

  private static final Gson GSON = new Gson();
  private OffsetAndSequence offsetMock;
  private String stateDataMock = "some state data";

  public MockAssessmentServiceClient(ServiceDiscoverer contexts) {
    super(contexts);
    setUp();
  }

  private void setUp() {
    Map<String, String> offsetState = new HashMap<>();
    offsetState.put("offset1", "test_me_lsn1");
    offsetMock = new OffsetAndSequence(new Offset(offsetState), 1);
  }

  @Override
  public <T> T retryableApiCall(String connectionUrl, String method, String requestBody, Type type) {

    //Mocks no matching offset data
    if (connectionUrl.equals("v1/contexts/namespace/apps/app/generations/100/instances/0/offset")
     && method.equals(HttpMethod.GET)) {
      return null;
    }

    //Mocks matching offset data for generation 110
    if (connectionUrl.equals("v1/contexts/namespace/apps/app/generations/110/instances/0/offset")
      && method.equals(HttpMethod.GET)) {
      return (T) offsetMock;
    }

    // Write offset mock
    if (connectionUrl.equals("v1/contexts/namespace/apps/app/generations/100/instances/0/offset")
      && method.equals(HttpMethod.PUT)) {
      return (T) compareRequestBody(GSON.toJson(offsetMock), requestBody, connectionUrl, method);
    }

    throw new IllegalStateException(" The given URL : " + method + " " + connectionUrl +
                                      "  doesn't match any of the expected URLs");
  }

  @Override
  public byte[] retryableApiCall(String connectionUrl, String method, byte[] requestBody) {

    if (connectionUrl.equals("v1/contexts/namespace/apps/app/generations/100/instances/0/states/test_state")
      && method.equals(HttpMethod.GET)) {
      return stateDataMock.getBytes(StandardCharsets.UTF_8);
    }

    // Write offset mock
    if (connectionUrl.equals("v1/contexts/namespace/apps/app/generations/100/instances/0/states/test_state")
      && method.equals(HttpMethod.PUT)) {
      return compareRequestBody(stateDataMock.getBytes(StandardCharsets.UTF_8), requestBody, connectionUrl, method);
    }

    throw new IllegalStateException(" The given URL : " + method + " " + connectionUrl +
                                      "  doesn't match any of the expected URLs");
  }

  public OffsetAndSequence getOffsetMock() {
    return offsetMock;
  }

  public String getStateDataMock() {
    return stateDataMock;
  }

  private String compareRequestBody(String expected, String actual, String connectionUrl, String method) {
    if (actual.equals(expected)) {
      return "";
    } else {
      throw new RuntimeException(" The given URL : " + method + " " + connectionUrl +
      " with the given Request Body doesn't match with the expected request body");
    }
  }

  private byte[] compareRequestBody(byte[] expected, byte[] actual, String connectionUrl, String method) {
    if (Arrays.equals(expected, actual)) {
      return new byte[0];
    } else {
      throw new RuntimeException(" The given URL : " + method + " " + connectionUrl +
                                   " with the given Request Body doesn't match with the expected request body");
    }
  }
}
