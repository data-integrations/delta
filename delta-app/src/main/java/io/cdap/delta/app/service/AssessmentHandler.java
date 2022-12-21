/*
 * Copyright © 2020-2022 Cask Data, Inc.
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

import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.worker.RemoteExecutionException;
import io.cdap.cdap.api.service.worker.RemoteTaskException;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.delta.api.DeltaPipelineId;
import io.cdap.delta.api.ReplicationError;
import io.cdap.delta.api.assessment.PipelineAssessment;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.app.DefaultConfigurer;
import io.cdap.delta.app.DeltaWorkerId;
import io.cdap.delta.app.PipelineStateService;
import io.cdap.delta.app.service.common.AbstractAssessorHandler;
import io.cdap.delta.app.service.remote.RemoteAssessPipelineTask;
import io.cdap.delta.app.service.remote.RemoteAssessTableTask;
import io.cdap.delta.app.service.remote.RemoteAssessmentRequest;
import io.cdap.delta.app.service.remote.RemoteAssessmentTaskBase;
import io.cdap.delta.app.service.remote.RemoteDescribeTableTask;
import io.cdap.delta.app.service.remote.RemoteListTablesTask;
import io.cdap.delta.proto.DBTable;
import io.cdap.delta.proto.DeltaConfig;
import io.cdap.delta.proto.DraftRequest;
import io.cdap.delta.proto.PipelineReplicationState;
import io.cdap.delta.proto.PipelineState;
import io.cdap.delta.proto.StateRequest;
import io.cdap.delta.proto.TableAssessmentRequest;
import io.cdap.delta.proto.TableAssessmentResponse;
import io.cdap.delta.proto.TableReplicationState;
import io.cdap.delta.store.Draft;
import io.cdap.delta.store.DraftId;
import io.cdap.delta.store.Namespace;
import io.cdap.delta.store.StateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handler for storing drafts and performing assessments.
 */
public class AssessmentHandler extends AbstractAssessorHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AssessmentHandler.class);

  @GET
  @Path("v1/contexts/{context}/drafts")
  public void listDrafts(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("context") String namespaceName) {
    respond(namespaceName, responder, (namespace) -> {
      List<Draft> drafts = getDraftService().listDrafts(namespace);
      responder.sendJson(drafts);
    });
  }

  @GET
  @Path("v1/contexts/{context}/drafts/{draft}")
  public void getDraft(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("context") String namespaceName,
                       @PathParam("draft") String draftName) {
    respond(namespaceName, responder, (namespace) -> {
      Draft draft = getDraftService().getDraft(new DraftId(namespace, draftName));
      responder.sendJson(draft);
    });
  }

  @PUT
  @Path("v1/contexts/{context}/drafts/{draft}")
  public void putDraft(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("context") String namespaceName,
                       @PathParam("draft") String draftName) {
    respond(namespaceName, responder, (namespace) -> {
      DraftRequest draft;
      try {
        draft = GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(), DraftRequest.class);
      } catch (JsonSyntaxException e) {
        LOG.error("Error in decoding request body:", e);
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Unable to decode request body: " + e.getMessage());
        return;
      } catch (IllegalArgumentException e) {
        LOG.error("Invalid request: ", e);
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Invalid config: " + e.getMessage());
        return;
      }

      getDraftService().saveDraft(new DraftId(namespace, draftName), draft);
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }

  @DELETE
  @Path("v1/contexts/{context}/drafts/{draft}")
  public void deleteDraft(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("context") String namespaceName,
                          @PathParam("draft") String draftName) {
    respond(namespaceName, responder, (namespace) -> {
      getDraftService().deleteDraft(new DraftId(namespace, draftName));
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }

  @POST
  @Path("v1/contexts/{context}/drafts/{draft}/listTables")
  public void listDraftTables(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("context") String namespaceName,
                              @PathParam("draft") String draftName) {
    respond(namespaceName, responder, (namespace) -> {
      DraftId draftId = new DraftId(namespace, draftName);
      Draft draft = getDraftService().getDraft(draftId);

      if (getContext().isRemoteTaskEnabled()) {
        executeRemotely(namespace, draft.getConfig(), null, RemoteListTablesTask.class, responder);
      } else {
        PluginConfigurer pluginConfigurer = getContext().createPluginConfigurer(namespaceName);
        TableList tableList = getDraftService().listDraftTables(draftId, draft,
                                                                new DefaultConfigurer(pluginConfigurer));
        responder.sendString(GSON.toJson(tableList));
      }
    });
  }

  @POST
  @Path("v1/contexts/{context}/drafts/{draft}/describeTable")
  public void describeTable(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("context") String namespaceName,
                            @PathParam("draft") String draftName) {
    respond(namespaceName, responder, (namespace) -> {
      String requestContent = StandardCharsets.UTF_8.decode(request.getContent()).toString();
      DBTable dbTable;
      try {
        dbTable = GSON.fromJson(requestContent, DBTable.class);
        dbTable.validate();
      } catch (JsonSyntaxException e) {
        LOG.error("Error in decoding request body:", e);
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Unable to decode request body: " + e.getMessage());
        return;
      } catch (IllegalArgumentException e) {
        LOG.error("Invalid request: ", e);
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Invalid request: " + e.getMessage());
        return;
      }

      DraftId draftId = new DraftId(namespace, draftName);
      Draft draft = getDraftService().getDraft(draftId);

      if (getContext().isRemoteTaskEnabled()) {
        executeRemotely(namespace, draft.getConfig(), requestContent, RemoteDescribeTableTask.class, responder);
      } else {
        PluginConfigurer pluginConfigurer = getContext().createPluginConfigurer(namespaceName);
        TableDetail tableDetail = getDraftService()
          .describeDraftTable(draftId, new DefaultConfigurer(pluginConfigurer), dbTable);
        responder.sendString(GSON.toJson(tableDetail));
      }
    });
  }

  @POST
  @Path("v1/contexts/{context}/drafts/{draft}/assessPipeline")
  public void assessDraft(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("context") String namespaceName,
                          @PathParam("draft") String draftName) {
    respond(namespaceName, responder, ((namespace) -> {
      DraftId draftId = new DraftId(namespace, draftName);
      Draft draft = getDraftService().getDraft(draftId);

      draft.getConfig().validatePipeline();

      if (getContext().isRemoteTaskEnabled()) {
        executeRemotely(namespace, draft.getConfig(), null, RemoteAssessPipelineTask.class, responder);
      } else {
        PluginConfigurer pluginConfigurer = getContext().createPluginConfigurer(namespaceName);
        PipelineAssessment assessment =
          getDraftService().assessPipeline(namespace, draft, new DefaultConfigurer(pluginConfigurer));
        responder.sendString(GSON.toJson(assessment));
      }
    }));
  }

  @POST
  @Path("v1/contexts/{context}/assessPipeline")
  public void assessDeltaConfig(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("context") String namespaceName) {
    respond(namespaceName, responder, ((namespace) -> {
      DeltaConfig deltaConfig;
      try {
        deltaConfig = GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(), DeltaConfig.class);
      } catch (JsonSyntaxException e) {
        LOG.error("Error in decoding request body:", e);
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Unable to decode request body , " +
          "doesn't conform to deltaConfig: " + e.getMessage());
        return;
      }
      deltaConfig.validatePipeline();

      if (getContext().isRemoteTaskEnabled()) {
        executeRemotely(namespace, deltaConfig, null, RemoteAssessPipelineTask.class, responder);
      } else {
        PluginConfigurer pluginConfigurer = getContext().createPluginConfigurer(namespaceName);
        PipelineAssessment assessment =
          getDraftService().assessPipeline(namespace, deltaConfig, new DefaultConfigurer(pluginConfigurer));
        responder.sendString(GSON.toJson(assessment));
      }
    }));
  }

  @POST
  @Path("v1/contexts/{context}/drafts/{draft}/assessTable")
  public void assessTable(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("context") String namespaceName,
                          @PathParam("draft") String draftName) {
    respond(namespaceName, responder, ((namespace) -> {
      DraftId draftId = new DraftId(namespace, draftName);
      String requestContent = StandardCharsets.UTF_8.decode(request.getContent()).toString();
      DBTable dbTable;
      try {
        dbTable = GSON.fromJson(requestContent, DBTable.class);
        dbTable.validate();
      } catch (JsonSyntaxException e) {
        LOG.error("Error in decoding request body:", e);
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Unable to decode request body: " + e.getMessage());
        return;
      } catch (IllegalArgumentException e) {
        LOG.error("Invalid request: ", e);
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Invalid request: " + e.getMessage());
        return;
      }

      Draft draft = getDraftService().getDraft(draftId);

      if (getContext().isRemoteTaskEnabled()) {
        executeRemotely(namespace, draft.getConfig(), requestContent, RemoteAssessTableTask.class, responder);
      } else {
        PluginConfigurer pluginConfigurer = getContext().createPluginConfigurer(namespaceName);
        TableAssessmentResponse assessment = getDraftService()
          .assessTable(namespace, draft, new DefaultConfigurer(pluginConfigurer), dbTable);
        responder.sendString(GSON.toJson(assessment));
      }
    }));
  }

  @POST
  @Path("v1/contexts/{context}/assessTable")
  public void assessTableWithDeltaConfig(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("context") String namespaceName) {
    respond(namespaceName, responder, ((namespace) -> {
      TableAssessmentRequest tableAssessmentRequest;
      try {
        tableAssessmentRequest = GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(),
                                               TableAssessmentRequest.class);
      } catch (JsonSyntaxException e) {
        LOG.error("Error in decoding request body:", e);
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Unable to decode request body , " +
          "doesn't conform to TableAssessmentRequest: " + e.getMessage());
        return;
      }

      DeltaConfig deltaConfig = tableAssessmentRequest.getDeltaConfig();
      DBTable dbTable = tableAssessmentRequest.getDBTable();

      if (getContext().isRemoteTaskEnabled()) {
        executeRemotely(namespace, deltaConfig, GSON.toJson(dbTable), RemoteAssessTableTask.class, responder);
      } else {
        PluginConfigurer pluginConfigurer = getContext().createPluginConfigurer(namespaceName);
        TableAssessmentResponse assessment = getDraftService()
          .assessTable(namespace, deltaConfig, new DefaultConfigurer(pluginConfigurer),
                       tableAssessmentRequest.getDBTable());
        responder.sendString(GSON.toJson(assessment));
      }
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
    respond(namespaceName, responder, ((namespace) -> {

      StateRequest stateRequest;
      try {
        stateRequest = GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(),
                                     StateRequest.class);
        stateRequest.validate();
      } catch (JsonSyntaxException e) {
        LOG.error("Error in decoding request body:", e);
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Unable to decode request body: " + e.getMessage());
        return;
      } catch (IllegalArgumentException e) {
        LOG.error("Invalid request: ", e);
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Invalid request: " + e.getMessage());
        return;
      }

      StateStore stateStore = getStateStore();

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
   * Common method for all remote executions.
   * Remote request is created, executed and response is added to {@link HttpServiceResponder}
   *
   * @param namespace                namespace string
   * @param config                   {@link DeltaConfig}
   * @param request                  Serialized request string if present
   * @param remoteExecutionTaskClass Remote execution task class
   * @param responder                {@link HttpServiceResponder} for the http request.
   */
  private void executeRemotely(Namespace namespace, DeltaConfig config, @Nullable String request,
                               Class<? extends RemoteAssessmentTaskBase> remoteExecutionTaskClass,
                               HttpServiceResponder responder) {

    DeltaConfig deltaConfig = getMacroEvaluator().evaluateMacros(namespace, config);
    RemoteAssessmentRequest remoteRequest = new RemoteAssessmentRequest(namespace.getName(), deltaConfig, request);
    RunnableTaskRequest runnableTaskRequest =
      RunnableTaskRequest.getBuilder(remoteExecutionTaskClass.getName()).
        withParam(GSON.toJson(remoteRequest)).
        build();
    try {
      byte[] bytes = getContext().runTask(runnableTaskRequest);
      responder.sendString(new String(bytes, StandardCharsets.UTF_8));
    } catch (RemoteExecutionException e) {
      LOG.error("Error in executing remote task " + remoteExecutionTaskClass, e);
      RemoteTaskException remoteTaskException = e.getCause();
      responder.sendError(
        getExceptionCode(remoteTaskException.getRemoteExceptionClassName()), remoteTaskException.getMessage());
    } catch (Exception e) {
      LOG.error("Error in executing remote task " + remoteExecutionTaskClass, e);
      responder.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
    }
  }

  private int getExceptionCode(String exceptionClass) {
    if (IllegalArgumentException.class.getName().equals(exceptionClass)
      || JsonSyntaxException.class.getName().equals(exceptionClass)) {
      return HttpURLConnection.HTTP_BAD_REQUEST;
    }
    return HttpURLConnection.HTTP_INTERNAL_ERROR;
  }
}
