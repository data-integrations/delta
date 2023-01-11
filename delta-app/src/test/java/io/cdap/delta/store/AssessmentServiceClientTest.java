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
import io.cdap.cdap.app.services.AbstractServiceDiscoverer;
import io.cdap.cdap.common.internal.remote.DefaultInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.service.ServiceDiscoverable;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.delta.api.Offset;
import io.cdap.delta.app.OffsetAndSequence;
import io.cdap.delta.app.service.AssessmentService;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

public class AssessmentServiceClientTest {

  private static final int RETRY_COUNT = 5;
  private static final Duration RETRY_DURATION = Duration.ofSeconds(5);
  private static NettyHttpService httpService;
  private static ServiceDiscoverer serviceDiscoverer;
  private static ProgramId programId;
  private static RemoteClientFactory remoteClientFactory;

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @BeforeClass
  public static void init() throws Exception {
    //Deploy a mock service to test RemoteHttpClient
    httpService = NettyHttpService.builder("dummy_service")
                                  .setHttpHandlers(new MockDummyServiceHandler())
                                  .build();

    httpService.start();
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    String discoveryName = ServiceDiscoverable.getName(NamespaceId.SYSTEM.getNamespace(),
                                                       "delta", ProgramType.SERVICE,
                                                       AssessmentService.NAME);
    discoveryService.register(new Discoverable(discoveryName, httpService.getBindAddress()));

    programId = new ProgramId(NamespaceId.SYSTEM.getNamespace(),
                                        "delta", ProgramType.SERVICE,
                                        AssessmentService.NAME);
    remoteClientFactory = new RemoteClientFactory(
      discoveryService, new DefaultInternalAuthenticator(new AuthenticationTestContext()));
    serviceDiscoverer = new AbstractServiceDiscoverer(programId) {
      @Override
      protected RemoteClientFactory getRemoteClientFactory() {
        return remoteClientFactory;
      }
    };
  }

  @Test
  public void testGETJsonCall() {
    Map<String, String> offsetState = new HashMap<>();
    offsetState.put("offset1", "test_me_lsn1");
    OffsetAndSequence offset = new OffsetAndSequence(new Offset(offsetState), 1);

    AssessmentServiceClient assessmentServiceClient = createAssessmentServiceClient();
    OffsetAndSequence obj =
      assessmentServiceClient.retryableApiCall("v1/testjson", HttpMethod.GET, OffsetAndSequence.class);
    Assert.assertEquals(offset, obj);
  }

  @Test
  public void testGETBytesCall() {
    AssessmentServiceClient assessmentServiceClient = createAssessmentServiceClient();
    byte[] result = assessmentServiceClient.retryableApiCall("v1/testbytes", HttpMethod.GET);
    byte[] data = "dummy text".getBytes(StandardCharsets.UTF_8);
    Assert.assertArrayEquals(data, result);
  }

  @Test
  public void testPOSTCall() {
    AssessmentServiceClient assessmentServiceClient = createAssessmentServiceClient();
    //The http status check in  AssessmentServiceClient will fail if not 200
    assessmentServiceClient.retryableApiCall("v1/test", HttpMethod.POST, "dummy body data");
  }

  //Mock a failure in the rest end point until it retries 3 times.
  @Test
  public void testRetries() {
    AssessmentServiceClient assessmentServiceClient = createAssessmentServiceClient();
    String retryCounter = assessmentServiceClient.retryableApiCall("v1/retry", HttpMethod.GET, String.class);
    Assert.assertEquals(String.valueOf(3), retryCounter);
  }

  @Test
  public void testTimeout() {
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage("Failed to call State Store (AssessorService) service with status 400: 0");

    AssessmentServiceClient assessmentServiceClient = createAssessmentServiceClient();
    assessmentServiceClient.retryableApiCall("v1/timeout", HttpMethod.GET, String.class);
  }

  @Test
  public void testExceptionOnConnectionOpen() {
    ServiceDiscoverer exceptionServiceDiscoverer = new AbstractServiceDiscoverer(programId) {
      @Override
      protected RemoteClientFactory getRemoteClientFactory() {
        return remoteClientFactory;
      }

      @Override
      public HttpURLConnection openConnection(String namespaceId, String applicationId, String serviceId,
                                              String methodPath) throws IOException {
        throw new UnknownHostException("host not found");
      }
    };

    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage("UnknownHostException");

    AssessmentServiceClient assessmentServiceClient = createAssessmentServiceClient(exceptionServiceDiscoverer);
    assessmentServiceClient.retryableApiCall("v1/testjson", HttpMethod.GET, String.class);
  }

  @Test
  public void testExceptionOnConnectionResponse() throws IOException {
    HttpURLConnection urlConnection = Mockito.mock(HttpURLConnection.class);
    ServiceDiscoverer exceptionServiceDiscoverer = new AbstractServiceDiscoverer(programId) {
      @Override
      protected RemoteClientFactory getRemoteClientFactory() {
        return remoteClientFactory;
      }

      @Override
      public HttpURLConnection openConnection(String namespaceId, String applicationId, String serviceId,
                                              String methodPath) throws IOException {
        return urlConnection;
      }
    };
    Mockito.when(urlConnection.getResponseCode()).thenThrow(new SocketTimeoutException("error"));

    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage("SocketTimeoutException");

    AssessmentServiceClient assessmentServiceClient = createAssessmentServiceClient(exceptionServiceDiscoverer);
    assessmentServiceClient.retryableApiCall("v1/testjson", HttpMethod.GET, String.class);
  }

  private AssessmentServiceClient createAssessmentServiceClient() {
    return createAssessmentServiceClient(serviceDiscoverer);
  }

  private AssessmentServiceClient createAssessmentServiceClient(ServiceDiscoverer discoverer) {
    return new AssessmentServiceClient(discoverer, RETRY_COUNT, RETRY_DURATION);
  }

  public static final class MockDummyServiceHandler extends AbstractHttpHandler {

    private int retryCounter = 0;

    @Path("/v3/namespaces/system/apps/delta/services/" + AssessmentService.NAME + "/methods/v1/testjson")
    @GET
    public void getJson(HttpRequest request, HttpResponder responder) {

      Map<String, String> offsetState = new HashMap<>();
      offsetState.put("offset1", "test_me_lsn1");
      OffsetAndSequence offset = new OffsetAndSequence(new Offset(offsetState), 1);
      responder.sendJson(HttpResponseStatus.OK, new Gson().toJson(offset));
    }

    @Path("/v3/namespaces/system/apps/delta/services/" + AssessmentService.NAME + "/methods/v1/testbytes")
    @GET
    public void getBytesArray(HttpRequest request, HttpResponder responder) {
      byte[] data = "dummy text".getBytes(StandardCharsets.UTF_8);
      responder.sendBytes(HttpResponseStatus.OK, ByteBuffer.wrap(data), new DefaultHttpHeaders());
    }

    @Path("/v3/namespaces/system/apps/delta/services/" + AssessmentService.NAME + "/methods/v1/test")
    @POST
    public void postDummy(FullHttpRequest request, HttpResponder responder) {
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @Path("/v3/namespaces/system/apps/delta/services/" + AssessmentService.NAME + "/methods/v1/retry")
    @GET
    public void retryApi(HttpRequest request, HttpResponder responder) {
      retryCounter++;
      if (retryCounter < 3) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, String.valueOf(retryCounter));
      } else {
        responder.sendString(HttpResponseStatus.OK, String.valueOf(retryCounter));
      }
    }

    @Path("/v3/namespaces/system/apps/delta/services/" + AssessmentService.NAME + "/methods/v1/timeout")
    @GET
    public void timeoutApi(HttpRequest request, HttpResponder responder) throws InterruptedException {
        Thread.sleep(5000);
        responder.sendString(HttpResponseStatus.BAD_REQUEST, String.valueOf(retryCounter));
    }
  }
}
