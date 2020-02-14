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

import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.DeltaTargetContext;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.ReplicationError;
import io.cdap.delta.proto.DBTable;
import io.cdap.delta.store.StateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import javax.annotation.Nullable;

/**
 * Context for delta plugins
 */
public class DeltaContext implements DeltaSourceContext, DeltaTargetContext {
  private static final String STATE_PREFIX = "state-";
  private static final Logger LOG = LoggerFactory.getLogger(DeltaContext.class);
  private final DeltaPipelineId id;
  private final String runId;
  private final Metrics metrics;
  private final StateStore stateStore;
  private final PluginContext pluginContext;
  private final EventMetrics eventMetrics;
  private final PipelineStateService stateService;
  private final int maxRetrySeconds;

  DeltaContext(DeltaPipelineId id, String runId, Metrics metrics, StateStore stateStore,
               PluginContext pluginContext, EventMetrics eventMetrics, PipelineStateService stateService,
               int maxRetrySeconds) {
    this.id = id;
    this.runId = runId;
    this.metrics = metrics;
    this.stateStore = stateStore;
    this.pluginContext = pluginContext;
    this.eventMetrics = eventMetrics;
    this.stateService = stateService;
    this.maxRetrySeconds = maxRetrySeconds;
  }

  @Override
  public void incrementCount(DMLOperation op) {
    eventMetrics.incrementDMLCount(op);
  }

  @Override
  public void incrementCount(DDLOperation op) {
    eventMetrics.incrementDDLCount();
  }

  @Override
  public void commitOffset(Offset offset, long sequenceNumber) throws IOException {
    stateStore.writeOffset(id, new OffsetAndSequence(offset, sequenceNumber));
    eventMetrics.emitMetrics();
  }

  @Override
  public void setTableError(String database, String table, ReplicationError error) throws IOException {
    stateService.setTableError(new DBTable(database, table), error);
  }

  @Override
  public void setTableReplicating(String database, String table) throws IOException {
    stateService.setTableReplicating(new DBTable(database, table));
  }

  @Override
  public void setTableSnapshotting(String database, String table) throws IOException {
    stateService.setTableSnapshotting(new DBTable(database, table));
  }

  @Override
  public void dropTableState(String database, String table) throws IOException {
    stateService.dropTable(new DBTable(database, table));
  }

  OffsetAndSequence loadOffset() throws IOException {
    OffsetAndSequence offset = stateStore.readOffset(id);
    return offset == null ? new OffsetAndSequence(new Offset(Collections.emptyMap()), 0L) : offset;
  }

  @Override
  public String getApplicationName() {
    return id.getApp();
  }

  @Override
  public String getRunId() {
    return runId;
  }

  @Override
  public Metrics getMetrics() {
    return metrics;
  }

  @Override
  public int getMaxRetrySeconds() {
    return maxRetrySeconds;
  }

  @Nullable
  @Override
  public byte[] getState(String key) throws IOException {
    return stateStore.readState(id, STATE_PREFIX + key);
  }

  @Override
  public void putState(String key, byte[] val) throws IOException {
    stateStore.writeState(id, STATE_PREFIX + key, val);
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId) {
    return pluginContext.getPluginProperties(pluginId);
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId, MacroEvaluator evaluator) throws InvalidMacroException {
    return pluginContext.getPluginProperties(pluginId, evaluator);
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    return pluginContext.loadPluginClass(pluginId);
  }

  @Override
  public <T> T newPluginInstance(String pluginId) throws InstantiationException {
    return pluginContext.newPluginInstance(pluginId);
  }

  @Override
  public <T> T newPluginInstance(String pluginId, MacroEvaluator evaluator)
    throws InstantiationException, InvalidMacroException {
    return pluginContext.newPluginInstance(pluginId, evaluator);
  }

  @Override
  public void setError(ReplicationError error) throws IOException {
    stateService.setSourceError(error);
  }

  @Override
  public void setOK() throws IOException {
    stateService.setSourceOK();
  }

  public void clearMetrics() {
    eventMetrics.clear();
  }
}
