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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.DeltaPipelineId;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.DeltaTargetContext;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.ReplicationError;
import io.cdap.delta.api.SourceProperties;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.proto.DBTable;
import io.cdap.delta.store.StateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Context for delta plugins
 */
public class DeltaContext implements DeltaSourceContext, DeltaTargetContext {
  private static final Logger LOG = LoggerFactory.getLogger(DeltaContext.class);
  private static final String STATE_PREFIX = "state-";
  private static final String PROGRAM_METRIC_ENTITY = "ent";
  private static final String DOT_SEPARATOR = ".";
  private final DeltaWorkerId id;
  private final String runId;
  private final Metrics metrics;
  private final StateStore stateStore;
  private final PluginContext pluginContext;
  private final Map<String, EventMetrics> tableEventMetrics;
  private final PipelineStateService stateService;
  private final int maxRetrySeconds;
  private final Map<String, String> runtimeArguments;
  private final AtomicReference<Throwable> failure;
  private final SourceProperties sourceProperties;
  private final Set<SourceTable> tables;
  private long sequenceNumber;

  DeltaContext(DeltaWorkerId id, String runId, Metrics metrics, StateStore stateStore,
               PluginContext pluginContext, PipelineStateService stateService,
               int maxRetrySeconds, Map<String, String> runtimeArguments,
               @Nullable SourceProperties sourceProperties, List<SourceTable> tables) {
    this.id = id;
    this.runId = runId;
    this.metrics = metrics;
    this.stateStore = stateStore;
    this.pluginContext = pluginContext;
    this.tableEventMetrics = new HashMap<>();
    this.stateService = stateService;
    this.maxRetrySeconds = maxRetrySeconds;
    this.runtimeArguments = Collections.unmodifiableMap(new HashMap<>(runtimeArguments));
    this.failure = new AtomicReference<>(null);
    this.sourceProperties = sourceProperties;
    this.tables = Collections.unmodifiableSet(new HashSet<>(tables));
  }

  @Override
  public void incrementCount(DMLOperation op) {
    getEventMetricsForTable(op.getDatabaseName(), op.getSchemaName(), op.getTableName()).incrementDMLCount(op);
  }

  @Override
  public void incrementCount(DDLOperation op) {
    String tableName = op.getTableName();
    if (tableName == null) {
      // This can happen for DDL operations such as CREATE_DATABASE
      return;
    }
    getEventMetricsForTable(op.getDatabaseName(), op.getSchemaName(), tableName).incrementDDLCount();
  }

  @Override
  public void commitOffset(Offset offset, long sequenceNumber) throws IOException {
    stateStore.writeOffset(id, new OffsetAndSequence(offset, sequenceNumber));
    for (EventMetrics eventMetrics : tableEventMetrics.values()) {
      eventMetrics.emitMetrics();
    }
    clearMetrics();
  }

  @Override
  public void setTableError(String database, String table, ReplicationError error) throws IOException {
    setTableError(database, null, table, error);
  }

  @Override
  public void setTableError(String database, @Nullable String schema, String table,
                            ReplicationError error) throws IOException {
    stateService.setTableError(new DBTable(database, schema, table), error);
    getEventMetricsForTable(database, schema, table).emitDMLErrorMetric();
  }

  private EventMetrics getEventMetricsForTable(String database, String schema, String table) {
    String fullyQualifiedTableName = Joiner.on(DOT_SEPARATOR).skipNulls().join(database, schema, table);
    return tableEventMetrics.computeIfAbsent(
      table, s -> new EventMetrics(metrics.child(ImmutableMap.of(PROGRAM_METRIC_ENTITY, fullyQualifiedTableName))));
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

  @Override
  public SourceProperties getSourceProperties() {
    return sourceProperties;
  }

  OffsetAndSequence loadOffset() throws IOException {
    OffsetAndSequence offset = stateStore.readOffset(id);
    return offset == null ? new OffsetAndSequence(new Offset(Collections.emptyMap()), sequenceNumber) : offset;
  }

  @Override
  public String getApplicationName() {
    return id.getPipelineId().getApp();
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
  public Map<String, String> getRuntimeArguments() {
    return runtimeArguments;
  }

  @Override
  public int getInstanceId() {
    return id.getInstanceId();
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
  public DeltaPipelineId getPipelineId() {
    return id.getPipelineId();
  }

  @Override
  public Set<SourceTable> getAllTables() {
    return tables;
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

  @Override
  public void initializeSequenceNumber(long sequenceNumber) {
    this.sequenceNumber = sequenceNumber;
  }

  @Override
  public void notifyFailed(Throwable cause) {
    failure.set(cause);
  }

  void clearMetrics() {
    tableEventMetrics.clear();
  }

  void throwFailureIfExists() throws Throwable {
    Throwable t = failure.getAndSet(null);
    if (t != null) {
      throw t;
    }
  }
}
