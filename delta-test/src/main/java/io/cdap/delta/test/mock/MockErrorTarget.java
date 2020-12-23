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
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaTarget;
import io.cdap.delta.api.DeltaTargetContext;
import io.cdap.delta.api.EventConsumer;
import io.cdap.delta.api.ReplicationError;
import io.cdap.delta.api.Sequenced;
import io.cdap.delta.api.assessment.StandardizedTableDetail;
import io.cdap.delta.api.assessment.TableAssessment;
import io.cdap.delta.api.assessment.TableAssessor;
import io.cdap.delta.proto.Artifact;
import io.cdap.delta.proto.Plugin;

import java.util.Collections;
import java.util.HashMap;

/**
 * Mock target used in unit tests. It only sets errors to context.
 */
@io.cdap.cdap.api.annotation.Plugin(type = DeltaTarget.PLUGIN_TYPE)
@Name(MockErrorTarget.NAME)
public class MockErrorTarget implements DeltaTarget {
  static final String NAME = "mockerror";

  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Conf conf;

  public MockErrorTarget(Conf conf) {
    this.conf = conf;
  }

  /**
   * Config for the plugin
   */
  private static class Conf extends PluginConfig {
  }

  @Override
  public void configure(Configurer configurer) {
    // no-op
  }

  @Override
  public EventConsumer createConsumer(DeltaTargetContext context) throws Exception {
    return new EventConsumer() {
      @Override
      public void start() {

      }

      @Override
      public void applyDDL(Sequenced<DDLEvent> event) throws Exception {

      }

      @Override
      public void applyDML(Sequenced<DMLEvent> event) throws Exception {
        DMLOperation operation = event.getEvent().getOperation();
        context.setTableError(operation.getDatabaseName(), operation.getTableName(),
                              new ReplicationError(new Exception()));
      }
    };
  }

  public static Plugin getPlugin() {
    return new Plugin(NAME, DeltaTarget.PLUGIN_TYPE, new HashMap<>(), Artifact.EMPTY);
  }

  @Override
  public TableAssessor<StandardizedTableDetail> createTableAssessor(Configurer configurer) throws Exception {
    return tableDetail -> new TableAssessment(Collections.emptyList(), Collections.emptyList());
  }

  private static PluginClass getPluginClass() {
    return new PluginClass(DeltaTarget.PLUGIN_TYPE, NAME, "", MockErrorTarget.class.getName(), "conf",
                           new HashMap<>());
  }
}
