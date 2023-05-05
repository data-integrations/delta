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
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.worker.WorkerContext;
import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.DeltaPipelineId;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.DeltaTargetContext;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.ReplicationError;
import io.cdap.delta.api.SourceProperties;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.app.metrics.MetricsHandler;
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

  private final DeltaWorkerId id;
  private final String runId;
  private final StateStore stateStore;
  private final WorkerContext workerContext;
  private final PipelineStateService stateService;
  private final int maxRetrySeconds;
  private final Map<String, String> runtimeArguments;
  private final AtomicReference<Throwable> failure;
  private final SourceProperties sourceProperties;
  private final Set<SourceTable> tables;
  private long sequenceNumber;
  private final AtomicReference<Offset> committedOffset;
  private final MetricsHandler metricsHandler;
  private final PipelineConfigService pipelineConfigService;

  DeltaContext(DeltaWorkerId id, String runId, Metrics metrics, StateStore stateStore,
               WorkerContext workerContext, PipelineStateService stateService,
               int maxRetrySeconds, Map<String, String> runtimeArguments,
               @Nullable SourceProperties sourceProperties, List<SourceTable> tables) {
    this.id = id;
    this.runId = runId;
    this.stateStore = stateStore;
    this.workerContext = workerContext;
    this.stateService = stateService;
    this.maxRetrySeconds = maxRetrySeconds;
    this.runtimeArguments = Collections.unmodifiableMap(new HashMap<>(runtimeArguments));
    this.failure = new AtomicReference<>(null);
    this.sourceProperties = sourceProperties;
    this.tables = Collections.unmodifiableSet(new HashSet<>(tables));
    this.committedOffset = new AtomicReference<>(null);
    this.pipelineConfigService = new PipelineConfigService(runtimeArguments);
    this.metricsHandler = new MetricsHandler(id, metrics, tables, pipelineConfigService);
  }

  @Override
  public void incrementCount(DMLOperation op) {
    metricsHandler.incrementConsumeCount(op);
  }

  @Override
  public void incrementCount(DDLOperation op) {
    String tableName = op.getTableName();
    if (tableName == null) {
      // This can happen for DDL operations such as CREATE_DATABASE
      return;
    }
    metricsHandler.incrementConsumeCount(op);
  }

  @Override
  public void incrementPublishCount(DMLOperation op) {
    metricsHandler.incrementPublishCount(op);
  }

  @Override
  public void incrementPublishCount(DDLOperation op) {
    String tableName = op.getTableName();
    if (tableName == null) {
      // This can happen for DDL operations such as CREATE_DATABASE
      return;
    }
    metricsHandler.incrementPublishCount(op);
  }

  @Override
  public void commitOffset(Offset offset, long sequenceNumber) throws IOException {
    stateStore.writeOffset(id, new OffsetAndSequence(offset, sequenceNumber));
    committedOffset.set(offset);
    metricsHandler.emitMetrics();
  }

  @Override
  public void setTableError(String database, String table, ReplicationError error) throws IOException {
    setTableError(database, null, table, error);
  }

  @Override
  public void setTableError(String database, @Nullable String schema, String table,
                            ReplicationError error) throws IOException {
    stateService.setTableError(new DBTable(database, schema, table), error);
    metricsHandler.emitDMLErrorMetric(database, schema, table);
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
    return metricsHandler.getMetrics();
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
  public int getInstanceCount() {
    return workerContext.getInstanceCount();
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
    return workerContext.getPluginProperties(pluginId);
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId, MacroEvaluator evaluator) throws InvalidMacroException {
    return workerContext.getPluginProperties(pluginId, evaluator);
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    return workerContext.loadPluginClass(pluginId);
  }

  @Override
  public <T> T newPluginInstance(String pluginId) throws InstantiationException {
    return workerContext.newPluginInstance(pluginId);
  }

  @Override
  public <T> T newPluginInstance(String pluginId, MacroEvaluator evaluator)
    throws InstantiationException, InvalidMacroException {
    return workerContext.newPluginInstance(pluginId, evaluator);
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
  public Offset getCommittedOffset() throws IOException {
    if (committedOffset.get() == null) {
      committedOffset.set(loadOffset().getOffset());
    }
    return committedOffset.get();
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
    metricsHandler.clearMetrics();
  }

  void close() throws InterruptedException {
    metricsHandler.close();
  }

  void throwFailureIfExists() throws Throwable {
    Throwable t = failure.getAndSet(null);
    if (t != null) {
      throw t;
    }
  }
}
