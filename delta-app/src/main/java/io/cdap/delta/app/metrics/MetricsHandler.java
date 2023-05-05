/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.delta.app.metrics;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.app.DeltaWorkerId;
import io.cdap.delta.app.Diagnostics;
import io.cdap.delta.app.EventMetrics;
import io.cdap.delta.app.PipelineConfigService;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Aggregate event count by table and emit/log metrics
 */
public class MetricsHandler {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsHandler.class);
  private static final String PROGRAM_METRIC_ENTITY = "ent";
  private static final String DOT_SEPARATOR = ".";
  private static final String SOURCE_PLUGIN = "SourcePlugin";
  private static final String TARGET_PLUGIN = "TargetPlugin";

  private final Metrics metrics;
  private PipelineConfigService pipelineConfigService;
  private final Map<String, EventMetrics> consumerTableEventMetrics;
  private final Map<String, AtomicReference<EventCounts>> tablePublishedEventCounts;
  private final Map<String, AtomicReference<EventCounts>> tableConsumedEventCounts;
  private final ScheduledExecutorService aggregateStatsExecutor;
  private final int logAggregateStatsIntervalSec;

  public MetricsHandler(DeltaWorkerId id, Metrics metrics, List<SourceTable> tables,
                        PipelineConfigService config) {
    this.metrics = metrics;
    this.pipelineConfigService = config;
    this.consumerTableEventMetrics = new HashMap<>();
    //HashMap is fine as we are not structurally modifying the map after initialization
    this.tablePublishedEventCounts = new HashMap<>();
    this.tableConsumedEventCounts = new HashMap<>();
    tables.forEach(table -> {
      String fullyQualifiedTableName =
        getFullyQualifiedTableName(table.getDatabase(), table.getSchema(), table.getTable());
      tablePublishedEventCounts.put(fullyQualifiedTableName, new AtomicReference<>(new EventCounts()));
      tableConsumedEventCounts.put(fullyQualifiedTableName, new AtomicReference<>(new EventCounts()));
    });

    String prefix = String.format("metrics-deltaworker-%d", id.getInstanceId());
    this.aggregateStatsExecutor = Executors.newScheduledThreadPool(1,
                                                                   Threads.createDaemonThreadFactory(prefix + "-%d"));

    this.logAggregateStatsIntervalSec = pipelineConfigService.getLogAggregateStatsIntervalSec();
    this.aggregateStatsExecutor.scheduleAtFixedRate(this::logEventStats, logAggregateStatsIntervalSec,
                                                    logAggregateStatsIntervalSec, TimeUnit.SECONDS);
  }

  public void emitMetrics() {
    consumerTableEventMetrics.forEach((table, eventMetrics) -> {
      EventCounts eventCounts = eventMetrics.getEventCounts();

      aggregateConsumeCounts(table, eventCounts);

      //Emit clears the counts so should be done after aggregation
      eventMetrics.emitMetrics();
    });
    consumerTableEventMetrics.clear();
  }

  public void incrementConsumeCount(DMLOperation op) {
    getEventMetricsForTable(op.getDatabaseName(), op.getSchemaName(), op.getTableName()).incrementDMLCount(op);
  }

  public void incrementConsumeCount(DDLOperation op) {
    getEventMetricsForTable(op.getDatabaseName(), op.getSchemaName(), op.getTableName()).incrementDDLCount();
  }

  public void incrementPublishCount(DMLOperation op) {
    String fullyQualifiedTableName = getFullyQualifiedTableName(op);
    AtomicReference<EventCounts> eventCountsRef = tablePublishedEventCounts.get(fullyQualifiedTableName);
    if (eventCountsRef != null) {
      eventCountsRef.get().incrementDMLCount(op.getType());
    }
  }

  public void incrementPublishCount(DDLOperation op) {
    String fullyQualifiedTableName = getFullyQualifiedTableName(op);
    AtomicReference<EventCounts> eventCountsRef = tablePublishedEventCounts.get(fullyQualifiedTableName);
    if (eventCountsRef != null) {
      eventCountsRef.get().incrementDDLCount();
    }
  }

  public void emitDMLErrorMetric(String database, String schema, String table) {
    getEventMetricsForTable(database, schema, table).emitDMLErrorMetric();
  }

  public Metrics getMetrics() {
    return metrics;
  }

  public void clearMetrics() {
    consumerTableEventMetrics.clear();
    tablePublishedEventCounts.values().forEach(value -> value.set(new EventCounts()));
    tableConsumedEventCounts.values().forEach(value -> value.set(new EventCounts()));
  }

  // TODO: Below code is thread unsafe, and assumes single threaded use for incrementConsumeCount
  // and emitMetrics methods. The assumption holds true today in case of consumer metrics,
  // but this should be replaced with ConcurrentHashMap in case of multi threaded access
  private EventMetrics getEventMetricsForTable(String database, String schema, String table) {
    String fullyQualifiedTableName = getFullyQualifiedTableName(database, schema, table);
    return consumerTableEventMetrics
      .computeIfAbsent(fullyQualifiedTableName, s -> new EventMetrics(
        metrics.child(ImmutableMap.of(PROGRAM_METRIC_ENTITY, fullyQualifiedTableName))));
  }

  private String getFullyQualifiedTableName(String database, String schema, String table) {
    return Joiner.on(DOT_SEPARATOR).skipNulls().join(database, schema, table);
  }

  private String getFullyQualifiedTableName(DMLOperation op) {
    return getFullyQualifiedTableName(op.getDatabaseName(), op.getSchemaName(), op.getTableName());
  }

  private String getFullyQualifiedTableName(DDLOperation op) {
    return getFullyQualifiedTableName(op.getDatabaseName(), op.getSchemaName(), op.getTableName());
  }

  private void aggregateConsumeCounts(String table, EventCounts eventCounts) {
    AtomicReference<EventCounts> aggregateCountRef = tableConsumedEventCounts.get(table);
    if (aggregateCountRef != null) {
      EventCounts agrregateEventCounts = aggregateCountRef.get();
      agrregateEventCounts.incrementDDLCount(eventCounts.getDdlEventCount());
      Map<DMLOperation.Type, AtomicInteger> dmlCounts = eventCounts.getDmlEventCounts();
      dmlCounts.forEach((type, count) ->
                          agrregateEventCounts.incrementDMLCount(type, count.get())
      );
    }
  }

  private void logEventStats() {
    logComponentStats(SOURCE_PLUGIN, tablePublishedEventCounts);
    logComponentStats(TARGET_PLUGIN, tableConsumedEventCounts);
  }

  private void logComponentStats(String component, Map<String, AtomicReference<EventCounts>> tableEventCounts) {
    AtomicInteger totalDdlEvents = new AtomicInteger(0);
    AtomicInteger totalDmlEvents = new AtomicInteger(0);

    List<String> tableStats = new ArrayList<>();
    tableEventCounts.forEach((table, counts) -> {
      EventCounts currEventCounts = counts.get();
      if (hasEvents(currEventCounts)) {
        //Replace only if there are events
        currEventCounts = counts.getAndSet(new EventCounts());
        totalDdlEvents.addAndGet(currEventCounts.getDdlEventCount());
        totalDmlEvents.addAndGet(getTotalDmlEvents(currEventCounts));
        tableStats.add(String.format("%s Stats [Table=%s] [%s]", component,
                                     table, getStats(currEventCounts)));
      }
    });
    LOG.info("{} Stats Summary [Interval={} sec] [DDL Events={}] [DML Events={}]",
             component, logAggregateStatsIntervalSec, totalDdlEvents, totalDmlEvents);
    tableStats.forEach(s -> LOG.info(s));

    //Log diagnostic info in case no events are processed by target plugin in the previous interval
    if (TARGET_PLUGIN.equals(component) &&
      (totalDdlEvents.get() + totalDmlEvents.get()) == 0 &&
      pipelineConfigService.isDiagnosticModeEnabled()) {
      LOG.info("Capturing diagnostic info as no events have been processed by " +
                 "target plugin in the last {} sec", logAggregateStatsIntervalSec);
      Diagnostics.logDiagnosticInfo();
    }
  }

  private int getTotalDmlEvents(EventCounts eventCounts) {
    int total = 0;
    for (AtomicInteger eventCount : eventCounts.getDmlEventCounts().values()) {
      total += eventCount.get();
    }
    return total;
  }

  private boolean hasEvents(EventCounts eventCounts) {
    if (eventCounts.getDdlEventCount() > 0) {
      return true;
    }
    Map<DMLOperation.Type, AtomicInteger> dmlEventCounts = eventCounts.getDmlEventCounts();
    return dmlEventCounts.values().stream().anyMatch(v -> v.get() > 0);
  }

  private String getStats(EventCounts eventCounts) {
    StringBuilder sb = new StringBuilder();

    sb.append("DDL Events=");
    sb.append(eventCounts.getDdlEventCount());
    sb.append(", DML Events=");

    eventCounts.getDmlEventCounts().forEach((op, count) -> {
      sb.append(' ');
      sb.append(op);
      sb.append(':');
      sb.append(count.get());
    });
    return sb.toString();
  }

  public void close() throws InterruptedException {
    aggregateStatsExecutor.shutdownNow();
    if (!aggregateStatsExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
      LOG.warn("Unable to cleanly shutdown aggregate stats executor  within the timeout.");
    }
  }
}
