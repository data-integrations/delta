/*
 * Copyright © 2019 Cask Data, Inc.
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
import io.cdap.delta.app.proto.Connection;
import io.cdap.delta.app.proto.DeltaConfig;
import io.cdap.delta.app.proto.Stage;

import java.util.List;

/**
 * App for Delta pipelines.
 */
public class DeltaApp extends AbstractApplication<DeltaConfig> {

  @Override
  public void configure() {
    DeltaConfig conf = getConfig();

    List<Connection> connections = conf.getConnections();
    if (connections.size() != 1 || conf.getStages().size() != 2) {
      throw new IllegalArgumentException("Delta pipelines currently only support a "
                                           + "single source connected to a single target");
    }

    Connection conn = connections.iterator().next();
    Stage sourceConf = conf.getStages().stream()
      .filter(s -> s.getName().equals(conn.getFrom()))
      .findAny()
      .orElseThrow(() -> new IllegalArgumentException(
        String.format("Source stage '%s' was not specified.", conn.getFrom())));
    Stage targetConf = conf.getStages().stream()
      .filter(s -> s.getName().equals(conn.getTo()))
      .findAny()
      .orElseThrow(() -> new IllegalArgumentException(
        String.format("Target stage '%s' was not specified.", conn.getTo())));

    Configurer configurer = new DefaultConfigurer(getConfigurer());
    DeltaSource source = registerPlugin(sourceConf);
    source.configure(configurer);
    DeltaTarget target = registerPlugin(targetConf);
    target.configure(configurer);

    addWorker(new DeltaWorker(sourceConf.getName(), targetConf.getName(), conf.getOffsetBasePath()));
  }

  private <T> T registerPlugin(Stage stageConf) {
    // TODO: use plugin artifact information
    return usePlugin(stageConf.getPlugin().getType(),
                     stageConf.getPlugin().getName(),
                     stageConf.getName(),
                     PluginProperties.builder().addAll(stageConf.getPlugin().getProperties()).build());
  }

}
