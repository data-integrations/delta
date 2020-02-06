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

package io.cdap.delta.test.mock;

import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.DeltaTarget;
import io.cdap.delta.api.DeltaTargetContext;
import io.cdap.delta.api.EventConsumer;
import io.cdap.delta.api.assessment.StandardizedTableDetail;
import io.cdap.delta.api.assessment.TableAssessment;
import io.cdap.delta.api.assessment.TableAssessor;
import io.cdap.delta.proto.Artifact;
import io.cdap.delta.proto.Plugin;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Mock target used in unit tests. Stores events to a file on the local filesystem.
 */
@io.cdap.cdap.api.annotation.Plugin(type = DeltaTarget.PLUGIN_TYPE)
@Name(MockTarget.NAME)
public class MockTarget implements DeltaTarget {
  static final String NAME = "mock";
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Conf conf;

  public MockTarget(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configure(Configurer configurer) {
    // no-op
  }

  @Override
  public EventConsumer createConsumer(DeltaTargetContext context) {
    File outputFile = new File(conf.path);
    if (outputFile.exists()) {
      outputFile.delete();
    }
    return new FileEventConsumer(outputFile, context);
  }

  @Override
  public TableAssessor<StandardizedTableDetail> createTableAssessor(Configurer configurer) {
    return tableDetail -> new TableAssessment(Collections.emptyList(), Collections.emptyList());
  }

  /**
   * Config for the plugin
   */
  private static class Conf extends PluginConfig {
    private String path;
  }

  /**
   * Get the plugin configuration for a mock target that should write events to a local file.
   * After the pipeline is stopped, use {@link FileEventConsumer#readEvents(File)} to read events written by the target.
   *
   * @param filePath path to the file to write events to
   * @return plugin configuration for the mock source
   */
  public static Plugin getPlugin(File filePath) {
    return new Plugin(NAME, DeltaTarget.PLUGIN_TYPE, Collections.singletonMap("path", filePath.getAbsolutePath()),
                      Artifact.EMPTY);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("path", new PluginPropertyField("path", "", "string", true, false));
    return new PluginClass(DeltaTarget.PLUGIN_TYPE, NAME, "", MockTarget.class.getName(), "conf", properties);
  }
}
