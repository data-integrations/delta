/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.delta.app.service.remote;

import com.google.gson.Gson;
import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.service.http.ServicePluginConfigurer;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.SystemAppTaskContext;
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.app.DefaultConfigurer;
import io.cdap.delta.proto.DeltaConfig;
import io.cdap.delta.proto.Plugin;
import io.cdap.delta.proto.Stage;
import io.cdap.delta.store.InvalidDraftException;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Base class for {@link RunnableTask} for assessment handling
 */
public abstract class RemoteAssessmentTaskBase implements RunnableTask {

  private static final Gson GSON = new Gson();

  @Override
  public void run(RunnableTaskContext context) throws Exception {
    //Get SystemAppTaskContext
    SystemAppTaskContext systemAppContext = context.getRunnableTaskSystemAppContext();
    //De serialize all requests
    RemoteAssessmentRequest connectionRequest = GSON.fromJson(context.getParam(),
                                                              RemoteAssessmentRequest.class);

    String executedResult = execute(systemAppContext, connectionRequest);
    context.writeResult(executedResult.getBytes(StandardCharsets.UTF_8));
  }

  protected DeltaSource getDeltaSource(DeltaConfig deltaConfig, Configurer configurer) {
    Stage stage = deltaConfig.getSource();

    Plugin pluginConfig = stage.getPlugin();
    DeltaSource deltaSource;
    try {
      deltaSource = configurer.usePlugin(pluginConfig.getType(), pluginConfig.getName(), UUID.randomUUID().toString(),
              PluginProperties.builder().addAll(pluginConfig.getProperties()).build());
    } catch (InvalidPluginConfigException e) {
      throw new InvalidDraftException(String.format("Unable to instantiate source plugin: %s", e.getMessage()), e);
    }

    if (deltaSource == null) {
      throw new InvalidDraftException(String.format("Unable to find plugin '%s'", pluginConfig.getName()));
    }
    return deltaSource;
  }

  /**
   * execute method for specific tasks to implement
   *
   * @param systemAppContext        {@link SystemAppTaskContext}
   * @param request                 {@link RemoteAssessmentRequest}
   * @return executed response as string
   * @throws Exception
   */
  public abstract String execute(SystemAppTaskContext systemAppContext,
                                 RemoteAssessmentRequest request) throws Exception;

  protected DefaultConfigurer getConfigurer(SystemAppTaskContext systemAppContext, String namespace) {
      ServicePluginConfigurer configurer = systemAppContext.createServicePluginConfigurer(namespace);
      DefaultConfigurer defaultConfigurer = new DefaultConfigurer(configurer);
      return defaultConfigurer;
  }
}
