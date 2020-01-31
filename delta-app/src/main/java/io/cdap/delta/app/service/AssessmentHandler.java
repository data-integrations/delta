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

package io.cdap.delta.app.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.service.http.AbstractSystemHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.delta.api.assessment.PipelineAssessment;
import io.cdap.delta.api.assessment.TableAssessment;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableNotFoundException;
import io.cdap.delta.app.DefaultConfigurer;
import io.cdap.delta.proto.CodedException;
import io.cdap.delta.proto.DraftRequest;
import io.cdap.delta.store.Draft;
import io.cdap.delta.store.DraftId;
import io.cdap.delta.store.DraftService;
import io.cdap.delta.store.Namespace;
import io.cdap.delta.store.SystemServicePropertyEvaluator;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.sql.SQLType;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handler for storing drafts and performing assessments.
 */
public class AssessmentHandler extends AbstractSystemHttpServiceHandler {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(SQLType.class, new SQLTypeSerializer())
    .setPrettyPrinting()
    .create();

  @GET
  @Path("v1/contexts/{context}/drafts")
  public void listDrafts(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("context") String namespaceName) {
    respond(namespaceName, responder, (draftService, namespace) -> {
      List<Draft> drafts = draftService.listDrafts(namespace);
      responder.sendJson(drafts);
    });
  }

  @GET
  @Path("v1/contexts/{context}/drafts/{draft}")
  public void getDraft(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("context") String namespaceName,
                       @PathParam("draft") String draftName) {
    respond(namespaceName, responder, (draftService, namespace) -> {
      Draft draft = draftService.getDraft(new DraftId(namespace, draftName));
      responder.sendJson(draft);
    });
  }

  @PUT
  @Path("v1/contexts/{context}/drafts/{draft}")
  public void putDraft(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("context") String namespaceName,
                       @PathParam("draft") String draftName) {
    respond(namespaceName, responder, (draftService, namespace) -> {
      DraftRequest draft;
      try {
        draft = GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(), DraftRequest.class);
      } catch (JsonSyntaxException e) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Unable to decode request body: " + e.getMessage());
        return;
      } catch (IllegalArgumentException e) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Invalid config: " + e.getMessage());
        return;
      }

      draftService.saveDraft(new DraftId(namespace, draftName), draft);
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }

  @POST
  @Path("v1/contexts/{context}/drafts/{draft}/listTables")
  public void listDraftTables(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("context") String namespaceName,
                              @PathParam("draft") String draftName) {
    respond(namespaceName, responder, (draftService, namespace) -> {
      DraftId draftId = new DraftId(namespace, draftName);
      PluginConfigurer pluginConfigurer = getContext().createPluginConfigurer(namespaceName);
      TableList tableList = draftService.listDraftTables(draftId, new DefaultConfigurer(pluginConfigurer));
      responder.sendString(GSON.toJson(tableList));
    });
  }

  @POST
  @Path("v1/contexts/{context}/drafts/{draft}/databases/{database}/tables/{table}/describe")
  public void describeTable(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("context") String namespaceName,
                            @PathParam("draft") String draftName,
                            @PathParam("database") String database,
                            @PathParam("table") String table) {
    respond(namespaceName, responder, (draftService, namespace) -> {
      DraftId draftId = new DraftId(namespace, draftName);
      PluginConfigurer pluginConfigurer = getContext().createPluginConfigurer(namespaceName);
      TableDetail tableDetail = draftService.describeDraftTable(draftId, new DefaultConfigurer(pluginConfigurer),
                                                                database, table);
      responder.sendString(GSON.toJson(tableDetail));
    });
  }

  @POST
  @Path("v1/contexts/{context}/drafts/{draft}/assess")
  public void assessDraft(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("context") String namespaceName,
                          @PathParam("draft") String draftName) {
    respond(namespaceName, responder, ((draftService, namespace) -> {
      DraftId draftId = new DraftId(namespace, draftName);
      PluginConfigurer pluginConfigurer = getContext().createPluginConfigurer(namespaceName);
      PipelineAssessment assessment = draftService.assessPipeline(draftId, new DefaultConfigurer(pluginConfigurer));
      responder.sendString(GSON.toJson(assessment));
    }));
  }

  @POST
  @Path("v1/contexts/{context}/drafts/{draft}/databases/{database}/tables/{table}/assess")
  public void assessTable(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("context") String namespaceName,
                          @PathParam("draft") String draftName,
                          @PathParam("database") String database,
                          @PathParam("table") String table) {
    respond(namespaceName, responder, ((draftService, namespace) -> {
      DraftId draftId = new DraftId(namespace, draftName);
      PluginConfigurer pluginConfigurer = getContext().createPluginConfigurer(namespaceName);
      TableAssessment assessment = draftService.assessTable(draftId, new DefaultConfigurer(pluginConfigurer),
                                                            database, table);
      responder.sendString(GSON.toJson(assessment));
    }));
  }

  /**
   * Utility method that checks that the namespace exists before responding.
   */
  private void respond(String namespaceName, HttpServiceResponder responder, NamespacedEndpoint endpoint) {
    SystemHttpServiceContext context = getContext();

    Namespace namespace;
    try {
      NamespaceSummary namespaceSummary = context.getAdmin().getNamespaceSummary(namespaceName);
      if (namespaceSummary == null) {
        responder.sendError(HttpURLConnection.HTTP_NOT_FOUND, String.format("Namespace '%s' not found", namespaceName));
        return;
      }
      namespace = new Namespace(namespaceSummary.getName(), namespaceSummary.getGeneration());
    } catch (IOException e) {
      responder.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR,
                          String.format("Unable to check if namespace '%s' exists.", namespaceName));
      return;
    }

    try {
      endpoint.respond(new DraftService(context, new SystemServicePropertyEvaluator(context)), namespace);
    } catch (CodedException e) {
      responder.sendError(e.getCode(), e.getMessage());
    } catch (TableNotFoundException e) {
      responder.sendError(HttpURLConnection.HTTP_NOT_FOUND, e.getMessage());
    } catch (Exception e) {
      responder.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
    }
  }

  /**
   * Encapsulates the core logic that needs to happen in an endpoint.
   */
  private interface NamespacedEndpoint {

    /**
     * Create the response that should be returned by the endpoint.
     */
    void respond(DraftService draftService, Namespace namespace) throws Exception;
  }
}
