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
import io.cdap.cdap.api.service.http.AbstractSystemHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.delta.api.assessment.ColumnAssessment;
import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.PipelineAssessment;
import io.cdap.delta.api.assessment.TableAssessment;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableSummary;
import io.cdap.delta.api.assessment.TableSummaryAssessment;
import io.cdap.delta.proto.DeltaConfig;
import io.cdap.delta.store.Draft;
import io.cdap.delta.store.DraftId;
import io.cdap.delta.store.DraftStore;
import io.cdap.delta.store.Namespace;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.sql.JDBCType;
import java.sql.SQLType;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
    respond(namespaceName, responder, namespace -> {
      List<Draft> drafts = TransactionRunners.run(getContext(), context -> {
        return DraftStore.get(context).listDrafts(namespace);
      });
      responder.sendJson(drafts);
    });
  }

  @GET
  @Path("v1/contexts/{context}/drafts/{draft}")
  public void getDraft(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("context") String namespaceName,
                       @PathParam("draft") String draftName) {
    respond(namespaceName, responder, namespace -> {
      Optional<Draft> draft = TransactionRunners.run(getContext(), context -> {
        return DraftStore.get(context).getDraft(new DraftId(namespace, draftName));
      });
      if (draft.isPresent()) {
        responder.sendJson(draft);
      } else {
        responder.sendError(HttpURLConnection.HTTP_NOT_FOUND,
                            String.format("Draft '%s' not found in namespace '%s'", draftName, namespaceName));
      }
    });
  }

  @PUT
  @Path("v1/contexts/{context}/drafts/{draft}")
  public void putDraft(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("context") String namespaceName,
                       @PathParam("draft") String draftName) {
    respond(namespaceName, responder, namespace -> {
      DeltaConfig config;
      try {
        config = GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(), DeltaConfig.class);
      } catch (JsonSyntaxException e) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Unable to decode request body: " + e.getMessage());
        return;
      } catch (IllegalArgumentException e) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Invalid config: " + e.getMessage());
        return;
      }

      TransactionRunners.run(getContext(), context -> {
        DraftStore draftStore = DraftStore.get(context);
        draftStore.writeDraft(new DraftId(namespace, draftName), config);
      });
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }

  @POST
  @Path("v1/contexts/{context}/drafts/{draft}/listTables")
  public void listDraftTables(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("context") String namespace,
                              @PathParam("draft") String draftName) {
    responder.sendString(GSON.toJson(new TableList(Collections.singletonList(new TableSummary("db", "table", 1)))));
  }

  @POST
  @Path("v1/contexts/{context}/databases/{database}/tables/{table}/describe")
  public void describeTable(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("context") String namespace,
                            @PathParam("database") String database,
                            @PathParam("table") String table) {
    responder.sendString(
      GSON.toJson(new TableDetail("db", "table",
                                  Collections.singletonList("id"),
                                  Collections.singletonList(new ColumnDetail("id", JDBCType.VARCHAR, false)))));
  }

  @POST
  @Path("v1/contexts/{context}/drafts/{draft}/assess")
  public void assessDraft(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("context") String namespace,
                          @PathParam("draft") String draftName) {
    responder.sendString(GSON.toJson(new PipelineAssessment(new TableSummaryAssessment("db", "table", 1, 0, 0),
                                                            Collections.emptyList(),
                                                            Collections.emptyList())));
  }

  @POST
  @Path("v1/contexts/{context}/databases/{database}/tables/{table}/assess")
  public void assessTable(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("context") String namespace,
                          @PathParam("database") String database,
                          @PathParam("table") String table) {
    responder.sendString(
      GSON.toJson(new TableAssessment(Collections.singletonList(new ColumnAssessment("id", JDBCType.VARCHAR)),
                                      Collections.emptyList(),
                                      Collections.emptyList())));
  }

  /**
   * Utility method that checks that the namespace exists before responding.
   */
  private void respond(String namespaceName, HttpServiceResponder responder, NamespacedEndpoint endpoint) {
    Namespace namespace;
    try {
      NamespaceSummary namespaceSummary = getContext().getAdmin().getNamespaceSummary(namespaceName);
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
      endpoint.respond(namespace);
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
    void respond(Namespace namespace) throws Exception;
  }
}
