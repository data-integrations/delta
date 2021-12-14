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

package io.cdap.delta.app.service;

import com.google.gson.JsonParseException;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.delta.api.DeltaPipelineId;
import io.cdap.delta.app.DeltaWorkerId;
import io.cdap.delta.app.OffsetAndSequence;
import io.cdap.delta.app.service.common.AbstractAssessorHandler;
import io.cdap.delta.proto.CodedException;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;

/**
 * Handler for storing and managing offset and state stores.
 */
public class OffsetStateHandler extends AbstractAssessorHandler {

  //Looks up the delta_offset_store table for offset data.
  @GET
  @Path("v1/contexts/{namespace}/apps/{app}/generations/{generation}/instances/{instanceid}/offset")
  public void getOffsetData(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("namespace") String namespaceName,
                            @PathParam("app") String app,
                            @PathParam("generation") long generation,
                            @PathParam("instanceid") int instanceid) {
    respond(namespaceName, responder, (namespace) -> {
      DeltaWorkerId id = new DeltaWorkerId(new DeltaPipelineId(namespaceName, app, generation), instanceid);
      OffsetAndSequence offset = getStateStore().readOffset(id);
      ByteBuffer bb = ByteBuffer.wrap(GSON.toJson(offset).getBytes(StandardCharsets.UTF_8));
      responder.send(HttpURLConnection.HTTP_OK, bb, MediaType.APPLICATION_JSON, new HashMap<>());
    });
  }

  //Upserts in the delta_offset_store table for offset data.
  @PUT
  @Path("v1/contexts/{namespace}/apps/{app}/generations/{generation}/instances/{instanceid}/offset")
  public void saveOffsetData(HttpServiceRequest request, HttpServiceResponder responder,
                             @PathParam("namespace") String namespaceName,
                             @PathParam("app") String app,
                             @PathParam("generation") long generation,
                             @PathParam("instanceid") int instanceid) {
    respond(namespaceName, responder, (namespace) -> {
      DeltaWorkerId id = new DeltaWorkerId(new DeltaPipelineId(namespaceName, app, generation), instanceid);
      getStateStore().writeOffset(id, convertBodyToOffset(request));
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }

  //Looks up the delta_state_store table for the given state
  @GET
  @Path("v1/contexts/{namespace}/apps/{app}/generations/{generation}/instances/{instanceid}/states/{state}")
  public void getStateData(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("namespace") String namespaceName,
                           @PathParam("app") String app,
                           @PathParam("generation") long generation,
                           @PathParam("instanceid") int instanceid,
                           @PathParam("state") String state) {
    respond(namespaceName, responder, (namespace) -> {
      DeltaWorkerId id = new DeltaWorkerId(new DeltaPipelineId(namespaceName, app, generation), instanceid);
      byte[] data = getStateStore().readState(id, state);
      responder
        .send(HttpURLConnection.HTTP_OK, ByteBuffer.wrap(data), MediaType.APPLICATION_OCTET_STREAM, new HashMap<>());
    });
  }

  // Upserts in the delta_state_store table for state data.
  @PUT
  @Path("v1/contexts/{namespace}/apps/{app}/generations/{generation}/instances/{instanceid}/states/{state}")
  public void saveStateData(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("namespace") String namespaceName,
                            @PathParam("app") String app,
                            @PathParam("generation") long generation,
                            @PathParam("instanceid") int instanceid,
                            @PathParam("state") String state) {

    respond(namespaceName, responder, (namespace) -> {
      DeltaWorkerId id = new DeltaWorkerId(new DeltaPipelineId(namespaceName, app, generation), instanceid);
      byte[] data = Bytes.toBytes(request.getContent());
      getStateStore().writeState(id, state, data);
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }

  //Fetches the latest generation for the given namespace and app.
  @GET
  @Path("v1/contexts/{namespace}/apps/{app}/maxgeneration")
  public void getMaxGeneration(HttpServiceRequest request, HttpServiceResponder responder,
                               @PathParam("namespace") String namespaceName,
                               @PathParam("app") String app) {
    respond(namespaceName, responder, (namespace) -> {
      responder.sendString(getStateStore().getLatestGeneration(namespaceName, app).toString());
    });
  }

  private OffsetAndSequence convertBodyToOffset(HttpServiceRequest request) {
    byte[] data = Bytes.toBytes(request.getContent());
    Reader targetReader = new InputStreamReader(new ByteArrayInputStream(data));

    OffsetAndSequence offset = null;
    try {
      offset = GSON.fromJson(targetReader, OffsetAndSequence.class);
    } catch (JsonParseException e) {
      throw new CodedException(HttpURLConnection.HTTP_BAD_REQUEST, "The offset json data " +
        "does not conform to class OffsetAndSequence ");
    }

    if (offset == null || offset.getOffset() == null) {
      throw new CodedException(HttpURLConnection.HTTP_PRECON_FAILED, "The offset json data can not be null");
    }

    return offset;
  }

}
