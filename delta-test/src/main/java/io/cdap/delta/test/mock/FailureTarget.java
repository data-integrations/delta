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
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DMLEvent;
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

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Mock target that can be configured to throw an exception after a certain number of events.
 */
@io.cdap.cdap.api.annotation.Plugin(type = DeltaTarget.PLUGIN_TYPE)
@Name(FailureTarget.NAME)
public class FailureTarget implements DeltaTarget {
  static final String NAME = "failure";
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Conf conf;

  public FailureTarget(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configure(Configurer configurer) {
    // no-op
  }

  @Override
  public EventConsumer createConsumer(DeltaTargetContext context) {
    File proceedFile = new File(conf.proceedFile);
    return new EventConsumer() {
      @Override
      public void start() {
        // no-op
      }

      @Override
      public void stop() {
        // no-op
      }

      @Override
      public void applyDDL(Sequenced<DDLEvent> event) {
        context.incrementCount(event.getEvent().getOperation());
        throwIfNeeded(event.getSequenceNumber(), event.getEvent().getDatabase(), event.getEvent().getTable());
        context.setTableReplicating(event.getEvent().getDatabase(), event.getEvent().getTable());
        context.commitOffset(event.getEvent().getOffset(), event.getSequenceNumber());
      }

      @Override
      public void applyDML(Sequenced<DMLEvent> event) {
        DMLEvent dml = event.getEvent();
        context.incrementCount(dml.getOperation());
        throwIfNeeded(event.getSequenceNumber(), dml.getDatabase(), dml.getTable());
        context.setTableReplicating(dml.getDatabase(), dml.getTable());
        context.commitOffset(dml.getOffset(), event.getSequenceNumber());
      }

      private void throwIfNeeded(long sequenceNum, String database, String table) {
        if (!proceedFile.exists() && sequenceNum > conf.sequenceNumThreshold) {
          RuntimeException e = new RuntimeException("Expected target failure for sequence number " + sequenceNum);
          context.setTableError(database, table, new ReplicationError(e));
          throw e;
        }
      }
    };
  }

  @Override
  public TableAssessor<StandardizedTableDetail> createTableAssessor(Configurer configurer) {
    return tableDetail -> new TableAssessment(Collections.emptyList(), Collections.emptyList());
  }

  /**
   * Config for the plugin
   */
  private static class Conf extends PluginConfig {

    private long sequenceNumThreshold;

    private String proceedFile;
  }

  /**
   * Get the plugin configuration for a mock target that should write events to a local file.
   * After the pipeline is stopped, use {@link FileEventConsumer#readEvents(File)} to read events written by the target.
   *
   * @param sequenceNumThreshold throw an exception if the sequence number is greater than this value
   * @param proceedFile stop throwing exceptions when the file exists
   * @return plugin configuration for the mock source
   */
  public static Plugin getPlugin(long sequenceNumThreshold, File proceedFile) {
    Map<String, String> properties = new HashMap<>();
    properties.put("sequenceNumThreshold", String.valueOf(sequenceNumThreshold));
    properties.put("proceedFile", proceedFile.getAbsolutePath());
    return new Plugin(NAME, DeltaTarget.PLUGIN_TYPE, properties, Artifact.EMPTY);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("sequenceNumThreshold", new PluginPropertyField("sequenceNumThreshold", "", "long", true, false));
    properties.put("proceedFile", new PluginPropertyField("proceedFile", "", "string", true, false));
    return new PluginClass(DeltaTarget.PLUGIN_TYPE, NAME, "", FailureTarget.class.getName(), "conf", properties);
  }
}
