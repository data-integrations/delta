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
import io.cdap.delta.api.ReplicationError;
import io.cdap.delta.api.assessment.PipelineAssessment;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableNotFoundException;
import io.cdap.delta.app.DefaultConfigurer;
import io.cdap.delta.app.DeltaPipelineId;
import io.cdap.delta.app.DeltaWorkerId;
import io.cdap.delta.app.PipelineStateService;
import io.cdap.delta.proto.CodedException;
import io.cdap.delta.proto.DBTable;
import io.cdap.delta.proto.DraftRequest;
import io.cdap.delta.proto.PipelineReplicationState;
import io.cdap.delta.proto.PipelineState;
import io.cdap.delta.proto.StateRequest;
import io.cdap.delta.proto.TableAssessmentResponse;
import io.cdap.delta.proto.TableReplicationState;
import io.cdap.delta.store.Draft;
import io.cdap.delta.store.DraftId;
import io.cdap.delta.store.DraftService;
import io.cdap.delta.store.Namespace;
import io.cdap.delta.store.StateStore;
import io.cdap.delta.store.SystemServicePropertyEvaluator;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.sql.SQLType;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.ws.rs.DELETE;
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
  private static final String OFFSET_PATH = "offset.base.path";
  // NOTE: this is only available in the configure() method
  private final String offsetBasePath;

  AssessmentHandler(String offsetBasePath) {
    this.offsetBasePath = offsetBasePath;
  }

  @Override
  protected void configure() {
    setProperties(Collections.singletonMap(OFFSET_PATH, offsetBasePath));
  }

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

  @DELETE
  @Path("v1/contexts/{context}/drafts/{draft}")
  public void deleteDraft(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("context") String namespaceName,
                          @PathParam("draft") String draftName) {
    respond(namespaceName, responder, (draftService, namespace) -> {
      draftService.deleteDraft(new DraftId(namespace, draftName));
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
  @Path("v1/contexts/{context}/drafts/{draft}/describeTable")
  public void describeTable(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("context") String namespaceName,
                            @PathParam("draft") String draftName) {
    respond(namespaceName, responder, (draftService, namespace) -> {
      DBTable dbTable;
      try {
        dbTable = GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(), DBTable.class);
        dbTable.validate();
      } catch (JsonSyntaxException e) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Unable to decode request body: " + e.getMessage());
        return;
      } catch (IllegalArgumentException e) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Invalid request: " + e.getMessage());
        return;
      }

      DraftId draftId = new DraftId(namespace, draftName);
      PluginConfigurer pluginConfigurer = getContext().createPluginConfigurer(namespaceName);
      TableDetail tableDetail = draftService
        .describeDraftTable(draftId, new DefaultConfigurer(pluginConfigurer), dbTable.getDatabase(), dbTable.getTable(),
          dbTable.getSchema());
      responder.sendString(GSON.toJson(tableDetail));
    });
  }

  @POST
  @Path("v1/contexts/{context}/drafts/{draft}/assessPipeline")
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
  @Path("v1/contexts/{context}/drafts/{draft}/assessTable")
  public void assessTable(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("context") String namespaceName,
                          @PathParam("draft") String draftName) {
    respond(namespaceName, responder, ((draftService, namespace) -> {
      DraftId draftId = new DraftId(namespace, draftName);
      DBTable dbTable;
      try {
        dbTable = GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(), DBTable.class);
        dbTable.validate();
      } catch (JsonSyntaxException e) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Unable to decode request body: " + e.getMessage());
        return;
      } catch (IllegalArgumentException e) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Invalid request: " + e.getMessage());
        return;
      }

      PluginConfigurer pluginConfigurer = getContext().createPluginConfigurer(namespaceName);
      TableAssessmentResponse assessment = draftService
        .assessTable(draftId, new DefaultConfigurer(pluginConfigurer), dbTable.getDatabase(), dbTable.getTable(),
          dbTable.getSchema());
      responder.sendString(GSON.toJson(assessment));
    }));
  }

  @GET
  @Path("health")
  public void healthCheck(HttpServiceRequest request, HttpServiceResponder responder) {
    responder.sendStatus(HttpURLConnection.HTTP_OK);
  }

  @POST
  @Path("v1/contexts/{context}/getState")
  public void getState(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("context") String namespaceName) {
    respond(namespaceName, responder, ((draftService, namespace) -> {

      StateRequest stateRequest;
      try {
        stateRequest = GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(),
                                     StateRequest.class);
        stateRequest.validate();
      } catch (JsonSyntaxException e) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Unable to decode request body: " + e.getMessage());
        return;
      } catch (IllegalArgumentException e) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Invalid request: " + e.getMessage());
        return;
      }

      // since offset base path can be set per replicator, allow the caller to pass in the base path
      // to override the default
      String offsetBasePath = stateRequest.getOffsetBasePath();
      if (offsetBasePath == null) {
        offsetBasePath = getContext().getSpecification().getProperty(OFFSET_PATH);
      }

      org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(offsetBasePath);
      StateStore stateStore = StateStore.from(path);

      Long latestGen = stateStore.getLatestGeneration(namespaceName, stateRequest.getName());
      // this can happen if the pipeline was never started
      if (latestGen == null) {
        responder.sendString(GSON.toJson(PipelineReplicationState.EMPTY));
        return;
      }

      DeltaPipelineId pipelineId = new DeltaPipelineId(namespaceName, stateRequest.getName(), latestGen);
      Collection<Integer> workerInstances = stateStore.getWorkerInstances(pipelineId);
      // this can happen if the pipeline was started but never got to a good state
      if (workerInstances.isEmpty()) {
        responder.sendString(GSON.toJson(PipelineReplicationState.EMPTY));
        return;
      }

      Set<TableReplicationState> tableStates = new HashSet<>();
      PipelineState sourceState = PipelineState.OK;
      ReplicationError sourceError = null;
      for (int instanceId : workerInstances) {
        PipelineStateService stateService = new PipelineStateService(new DeltaWorkerId(pipelineId, instanceId),
                                                                     stateStore);
        stateService.load();
        PipelineReplicationState instanceState = stateService.getState();
        tableStates.addAll(instanceState.getTables());
        // if one instance is in error state, the entire replicator is in error state
        sourceState = sourceState == PipelineState.FAILING ? sourceState : instanceState.getSourceState();
        // if one instance has a replication error, keep that as the error for the entire replicator
        sourceError = sourceError == null ? instanceState.getSourceError() : sourceError;
      }
      responder.sendString(GSON.toJson(new PipelineReplicationState(sourceState, tableStates, sourceError)));
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
