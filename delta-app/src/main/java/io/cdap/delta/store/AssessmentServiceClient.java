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

import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.delta.app.service.AssessmentService;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.FailsafeExecutor;
import net.jodah.failsafe.RetryPolicy;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import javax.annotation.Nullable;

/**
 * Class to handle Api calls to System Service
 */
public class AssessmentServiceClient {

  private final ServiceDiscoverer serviceDiscoverer;
  private final RetryPolicy retryPolicy;
  private static final Gson GSON = new Gson();
  private static final String SERVICE_NAME = "State Store (AssessorService)";

  public AssessmentServiceClient(ServiceDiscoverer context) {
    this.serviceDiscoverer = context;
    retryPolicy = new RetryPolicy()
      .withMaxAttempts(3)
      .withMaxDuration(Duration.of(10, ChronoUnit.SECONDS))
      .withDelay(Duration.of(200, ChronoUnit.MILLIS))
      .withJitter(0.20D);
  }

  // Api calls to deal directly with byte[]
  public byte[] retryableApiCall(String connectionUrl, String method, @Nullable byte[] requestBody) {
    FailsafeExecutor<byte[]> failsafe = Failsafe.with(retryPolicy);
    return failsafe.get(() -> executeCall(getHttpUrlConnection(connectionUrl), method, requestBody));
  }

  public byte[] retryableApiCall(String connectionUrl, String method) {
    return retryableApiCall(connectionUrl, method, new byte[0]);
  }

  private byte[] executeCall(HttpURLConnection urlConn, String method, @Nullable byte[] body) throws IOException {
    urlConn.setRequestMethod(method);
    //Set request body if body is not null
    if (body != null && body.length != 0) {
      urlConn.setDoOutput(true);
      try (OutputStream os = urlConn.getOutputStream()) {
        os.write(body);
      }
    }
    return retrieve(urlConn);
  }

  private byte[] retrieve(HttpURLConnection urlConn) throws IOException {
    checkResponseCode(urlConn);

    try (InputStream inputStream = urlConn.getInputStream()) {
      return ByteStreams.toByteArray(inputStream);
    } finally {
      urlConn.disconnect();
    }
  }

  // Api calls to deal with Objects
  public <T> T retryableApiCall(String connectionUrl, String method, @Nullable String requestBody,
                                @Nullable Type type) {
    FailsafeExecutor<T> failsafe = Failsafe.with(retryPolicy);
    return failsafe.get(() ->  executeCall(getHttpUrlConnection(connectionUrl), method, requestBody, type));
  }

  public <T> T  retryableApiCall(String connectionUrl, String method, Type type) {
    return retryableApiCall(connectionUrl, method, null, type);
  }

  public void retryableApiCall(String connectionUrl, String method, @Nullable String requestBody) {
    retryableApiCall(connectionUrl, method, requestBody, null);
  }

  private <T> T executeCall(HttpURLConnection urlConn, String method, @Nullable String body, Type type)
    throws IOException {
    urlConn.setRequestMethod(method);
    if (body != null) {
      urlConn.setDoOutput(true);
      try (OutputStreamWriter osw = new OutputStreamWriter(urlConn.getOutputStream(), StandardCharsets.UTF_8)) {
        osw.write(body);
      }
    }
    return retrieve(urlConn, type);
  }

  private <T> T retrieve(HttpURLConnection urlConn, @Nullable Type type) throws IOException {
    checkResponseCode(urlConn);
    // type is null means it is a POST/PUT call with body to be updated.
    // No particular response is expected
    // The "checkResponseCode" method ensures the status code to be HttpURLConnection.HTTP_OK.
    if (type == null) {
      urlConn.disconnect();
      return (T) Integer.valueOf(HttpURLConnection.HTTP_OK);
    }

    try (InputStreamReader reader = new InputStreamReader(urlConn.getInputStream(), StandardCharsets.UTF_8)) {
      return GSON.fromJson(reader, type);
    } finally {
      urlConn.disconnect();
    }
  }

  private void checkResponseCode(HttpURLConnection urlConn) throws IOException {
    int responseCode = urlConn.getResponseCode();
    if (responseCode != HttpURLConnection.HTTP_OK) {
      throw new RetryableException("Failed to call " + SERVICE_NAME + " service with status " + responseCode + ": " +
                                     getError(urlConn));
    }
  }

  // Returns the full content of the error stream for the given {@link HttpURLConnection}.
  private String getError(HttpURLConnection urlConn) {
    try (InputStream is = urlConn.getErrorStream()) {
      if (is == null) {
        return "Unknown error";
      }
      return new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8);
    } catch (IOException e) {
      return "Unknown error due to failure to read from error output: " + e.getMessage();
    }
  }

  // A connection object is created with the given System Service paths and parameters
  private HttpURLConnection getHttpUrlConnection(String methodPath) throws IOException {
    HttpURLConnection connectionUrl = serviceDiscoverer.openConnection("system",
                                                                       "delta",
                                                                       AssessmentService.NAME,
                                                                       methodPath);
    if (connectionUrl == null) {
      throw new RetryableException(SERVICE_NAME + " service is not available");
    }
    return connectionUrl;
  }

}

