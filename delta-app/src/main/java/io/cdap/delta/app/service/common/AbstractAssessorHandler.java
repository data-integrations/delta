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

package io.cdap.delta.app.service.common;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.service.http.AbstractSystemHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.delta.api.assessment.TableNotFoundException;
import io.cdap.delta.app.service.SQLTypeSerializer;
import io.cdap.delta.macros.MacroEvaluator;
import io.cdap.delta.proto.CodedException;
import io.cdap.delta.store.DBStateStoreService;
import io.cdap.delta.store.DraftService;
import io.cdap.delta.store.Namespace;
import io.cdap.delta.store.StateStore;
import io.cdap.delta.store.SystemServicePropertyEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.sql.SQLType;

/**
 * Common functionality for Assessor services.
 */
public class AbstractAssessorHandler extends AbstractSystemHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractAssessorHandler.class);

  protected static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(SQLType.class, new SQLTypeSerializer())
    .setPrettyPrinting()
    .create();

  @Override
  protected void configure() {
    //no-op
  }

  /**
   * Utility method that checks that the namespace exists before responding.
   */
  protected void respond(String namespaceName, HttpServiceResponder responder, NamespacedEndpoint endpoint) {
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
      LOG.error("Error in getting namespace details", e);
      responder.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR,
                          String.format("Unable to check if namespace '%s' exists.", namespaceName));
      return;
    }

    try {
      endpoint.respond(namespace);
    } catch (CodedException e) {
      LOG.error("Error in assessment handler", e);
      responder.sendError(e.getCode(), e.getMessage());
    } catch (TableNotFoundException e) {
      LOG.error("Error in getting table details", e);
      responder.sendError(HttpURLConnection.HTTP_NOT_FOUND, e.getMessage());
    } catch (Exception e) {
      LOG.error("Internal Error", e);
      responder.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
    }
  }

  protected DraftService getDraftService() {
    SystemHttpServiceContext context = getContext();
    return new DraftService(context, getMacroEvaluator(context));
  }

  protected MacroEvaluator getMacroEvaluator() {
    SystemHttpServiceContext context = getContext();
    return getMacroEvaluator(context);
  }

  protected MacroEvaluator getMacroEvaluator(SystemHttpServiceContext context) {
    return new MacroEvaluator(new SystemServicePropertyEvaluator(context));
  }

  protected StateStore getStateStore() {
    SystemHttpServiceContext context = getContext();
    return new DBStateStoreService(context);
  }

  /**
   * Encapsulates the core logic that needs to happen in an endpoint.
   */
  protected interface NamespacedEndpoint {

    /**
     * Create the response that should be returned by the endpoint.
     */
    void respond(Namespace namespace) throws Exception;
  }
}
