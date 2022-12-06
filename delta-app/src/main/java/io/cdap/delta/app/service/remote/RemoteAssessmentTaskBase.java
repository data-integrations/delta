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
import io.cdap.cdap.api.service.http.ServicePluginConfigurer;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.SystemAppTaskContext;
import io.cdap.delta.app.DefaultConfigurer;
import io.cdap.delta.app.service.Assessor;

import java.nio.charset.StandardCharsets;

/**
 * Base class for {@link RunnableTask} for assessment handling
 */
public abstract class RemoteAssessmentTaskBase implements RunnableTask {

  protected static final Gson GSON = new Gson();
  private Assessor assessor = new Assessor();

  @Override
  public void run(RunnableTaskContext context) throws Exception {
    SystemAppTaskContext systemAppContext = context.getRunnableTaskSystemAppContext();
    RemoteAssessmentRequest connectionRequest = GSON.fromJson(context.getParam(), RemoteAssessmentRequest.class);

    String executedResult = execute(systemAppContext, connectionRequest);
    context.writeResult(executedResult.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * execute method for specific tasks to implement
   *
   * @param systemAppContext {@link SystemAppTaskContext}
   * @param request          {@link RemoteAssessmentRequest}
   * @return executed response as string
   * @throws Exception
   */
  protected abstract String execute(SystemAppTaskContext systemAppContext,
                                    RemoteAssessmentRequest request) throws Exception;

  protected DefaultConfigurer getConfigurer(SystemAppTaskContext systemAppContext, String namespace) {
    ServicePluginConfigurer configurer = systemAppContext.createServicePluginConfigurer(namespace);
    return new DefaultConfigurer(configurer);
  }

  protected Assessor getAssessor() {
    return assessor;
  }
}
