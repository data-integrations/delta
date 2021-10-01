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

package io.cdap.delta.app;

import io.cdap.cdap.api.annotation.Requirements;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.app.ApplicationUpdateContext;
import io.cdap.cdap.api.app.ApplicationUpdateResult;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaTarget;
import io.cdap.delta.app.service.AssessmentService;
import io.cdap.delta.app.service.DefaultSourceConfigurer;
import io.cdap.delta.proto.ColumnTransformation;
import io.cdap.delta.proto.DeltaConfig;
import io.cdap.delta.proto.Stage;
import io.cdap.transformation.TransformationUtil;
import io.cdap.transformation.api.Transformation;

/**
 * App for Delta pipelines.
 */
@Requirements(capabilities = "cdc")
public class DeltaApp extends AbstractApplication<DeltaConfig> {

  @Override
  public void configure() {
    DeltaConfig conf = getConfig();

    if (conf.isService()) {
      addService(new AssessmentService(conf.getOffsetBasePath()));
      setDescription("Delta Pipeline System Service");
      return;
    }

    conf.validatePipeline();
    Stage sourceConf = conf.getSource();
    Stage targetConf = conf.getTarget();

    DeltaSource source = registerPlugin(sourceConf);
    Configurer configurer = new DefaultConfigurer(getConfigurer());
    DefaultSourceConfigurer sourceConfigurer = new DefaultSourceConfigurer(configurer);
    source.configure(sourceConfigurer);
    DeltaTarget target = registerPlugin(targetConf);
    target.configure(configurer);
    conf.getTableLevelTransformations().forEach(t -> t.getColumnLevelTransformations()
                                                       .forEach(this::registerTransformationPlugin));

    addWorker(new DeltaWorker(conf, sourceConfigurer.getSourceProperties()));

    String description = conf.getDescription();
    if (description == null) {
      description = String.format("%s to %s", sourceConf.getName(), targetConf.getName());
    }
    setDescription(description);
  }

  private <T> T registerPlugin(Stage stageConf) {
    // TODO: use plugin artifact information
    return usePlugin(stageConf.getPlugin().getType(),
                     stageConf.getPlugin().getName(),
                     stageConf.getName(),
                     PluginProperties.builder().addAll(stageConf.getPlugin().getProperties()).build());
  }
  private void registerTransformationPlugin(ColumnTransformation transformation) {
    String directiveName = TransformationUtil.parseDirectiveName(transformation.getTransformation());
    usePlugin(Transformation.PLUGIN_TYPE, directiveName, directiveName, PluginProperties.builder().build());
  }

  @Override
  public boolean isUpdateSupported() {
    return true;
  }

  @Override
  public ApplicationUpdateResult<DeltaConfig> updateConfig(ApplicationUpdateContext updateContext)
    throws Exception {
    DeltaConfig currentConfig = updateContext.getConfig(DeltaConfig.class);
    DeltaConfig updatedConfig = currentConfig.updateConfig(updateContext);
    return new ApplicationUpdateResult<>(updatedConfig);
  }
}
