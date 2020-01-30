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

import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaTarget;
import io.cdap.delta.app.service.AssessmentService;
import io.cdap.delta.proto.DeltaConfig;
import io.cdap.delta.proto.Stage;

/**
 * App for Delta pipelines.
 */
public class DeltaApp extends AbstractApplication<DeltaConfig> {

  @Override
  public void configure() {
    DeltaConfig conf = getConfig();

    if (conf.isService()) {
      addService(new AssessmentService());
      setDescription("Delta Pipeline System Service");
      return;
    }

    conf.validatePipeline();
    Stage sourceConf = conf.getSource();
    Stage targetConf = conf.getTarget();

    DeltaSource source = registerPlugin(sourceConf);
    Configurer configurer = new DefaultConfigurer(getConfigurer());
    source.configure(configurer);
    DeltaTarget target = registerPlugin(targetConf);
    target.configure(configurer);

    addWorker(new DeltaWorker(sourceConf.getName(), targetConf.getName(), conf.getOffsetBasePath(),
                              conf.getTables()));

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

}
