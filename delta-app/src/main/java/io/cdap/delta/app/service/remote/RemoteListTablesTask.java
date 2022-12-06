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
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.SystemAppTaskContext;
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.app.service.Assessor;
import io.cdap.delta.proto.DeltaConfig;

/**
 * {@link RunnableTask} for executing list tables remotely
 */
public class RemoteListTablesTask extends RemoteAssessmentTaskBase {
    private static final Gson GSON = new Gson();

    @Override
    public String execute(SystemAppTaskContext systemAppContext,
                          RemoteAssessmentRequest request) throws Exception {
        String namespace = request.getNamespace();
        DeltaConfig deltaConfig = request.getConfig();

        Configurer configurer = getConfigurer(systemAppContext, namespace);
        Assessor pluginAssesor = new Assessor();

        TableList tableList = pluginAssesor.listTables(namespace, deltaConfig, configurer);
        return GSON.toJson(tableList);
    }
}
