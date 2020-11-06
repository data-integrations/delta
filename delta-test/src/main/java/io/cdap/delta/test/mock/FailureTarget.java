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
import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DeltaFailureException;
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
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

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
    File proceedFile = conf.proceedFile == null ? null : new File(conf.proceedFile);
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
      public void applyDDL(Sequenced<DDLEvent> event) throws IOException, DeltaFailureException {
        DDLOperation ddlOperation = event.getEvent().getOperation();
        context.incrementCount(ddlOperation);
        throwIfNeeded(event.getSequenceNumber(), event.getEvent().getDatabase(), ddlOperation.getTableName());
        context.setTableReplicating(event.getEvent().getDatabase(), ddlOperation.getTableName());
        context.commitOffset(event.getEvent().getOffset(), event.getSequenceNumber());
      }

      @Override
      public void applyDML(Sequenced<DMLEvent> event) throws IOException, DeltaFailureException {
        DMLEvent dml = event.getEvent();
        context.incrementCount(dml.getOperation());
        throwIfNeeded(event.getSequenceNumber(), dml.getDatabase(), dml.getOperation().getTableName());
        context.setTableReplicating(dml.getDatabase(), dml.getOperation().getTableName());
        context.commitOffset(dml.getOffset(), event.getSequenceNumber());
      }

      private void throwIfNeeded(long sequenceNum, String database, String table)
        throws IOException, DeltaFailureException {
        if ((proceedFile == null || !proceedFile.exists()) && sequenceNum > conf.sequenceNumThreshold) {
          RuntimeException e = new RuntimeException("Expected target failure for sequence number " + sequenceNum);
          context.setTableError(database, table, new ReplicationError(e));
          if (conf.failImmediately) {
            throw new DeltaFailureException(e.getMessage(), e);
          }
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

    @Nullable
    private String proceedFile;

    private boolean failImmediately;
  }

  /**
   * Get the plugin configuration for a mock target that should fail after it sees an event with a sequence number
   * greater than the given threshold. The target will stop throwing exceptions after the given file exists.
   *
   * @param sequenceNumThreshold throw an exception if the sequence number is greater than this value
   * @param proceedFile stop throwing exceptions when the file exists
   * @return plugin configuration for the mock source
   */
  public static Plugin failAfter(long sequenceNumThreshold, File proceedFile) {
    Map<String, String> properties = new HashMap<>();
    properties.put("sequenceNumThreshold", String.valueOf(sequenceNumThreshold));
    properties.put("proceedFile", proceedFile.getAbsolutePath());
    properties.put("failImmediately", String.valueOf(Boolean.FALSE));
    return new Plugin(NAME, DeltaTarget.PLUGIN_TYPE, properties, Artifact.EMPTY);
  }

  /**
   * Get the plugin configuration for a mock target that should cause the pipeline to fail immediately without retries
   * when it sees an event with a sequence number greater than the given threshold.
   *
   * @param sequenceNumThreshold immediately fail the pipeline if the sequence number is greater than this value
   * @return plugin configuration for the mock source
   */
  public static Plugin failImmediately(long sequenceNumThreshold) {
    Map<String, String> properties = new HashMap<>();
    properties.put("sequenceNumThreshold", String.valueOf(sequenceNumThreshold));
    properties.put("failImmediately", String.valueOf(Boolean.TRUE));
    return new Plugin(NAME, DeltaTarget.PLUGIN_TYPE, properties, Artifact.EMPTY);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("sequenceNumThreshold", new PluginPropertyField("sequenceNumThreshold", "", "long", true, false));
    properties.put("proceedFile", new PluginPropertyField("proceedFile", "", "string", false, false));
    properties.put("failImmediately", new PluginPropertyField("failImmediately", "", "boolean", true, false));
    return new PluginClass(DeltaTarget.PLUGIN_TYPE, NAME, "", FailureTarget.class.getName(), "conf", properties);
  }
}
