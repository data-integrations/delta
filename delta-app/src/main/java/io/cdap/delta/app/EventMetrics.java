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

package io.cdap.delta.app;

import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.app.metrics.EventCounts;

/**
 * Emits metrics about DDL and DML events.
 */
public class EventMetrics {
  private final Metrics metrics;
  private EventCounts eventCounts;
  private long oldestTimeStampInMillis;
  private int bytesProcessed;

  public EventMetrics(Metrics metrics) {
    this.metrics = metrics;
    this.eventCounts = new EventCounts();
  }

  public synchronized void incrementDMLCount(DMLOperation op) {
    eventCounts.incrementDMLCount(op.getType());
    if (oldestTimeStampInMillis == 0) {
      oldestTimeStampInMillis = op.getIngestTimestampMillis();
    } else {
      oldestTimeStampInMillis = Math.min(op.getIngestTimestampMillis(), oldestTimeStampInMillis);
    }
    bytesProcessed += op.getSizeInBytes();
  }

  public synchronized void incrementDDLCount() {
    eventCounts.incrementDDLCount();
  }

  public synchronized void emitDMLErrorMetric() {
    metrics.count("dml.errors", 1);
  }

  public synchronized void emitMetrics() {
    for (DMLOperation.Type op : eventCounts.getDmlEventCounts().keySet()) {
      metrics.count(String.format("dml.%ss", op.name().toLowerCase()), eventCounts.getDMLCount(op));
    }
    metrics.count("ddl", eventCounts.getDdlEventCount());

    metrics.count("dml.data.processed.bytes", bytesProcessed);
    metrics.gauge("dml.latency.seconds", oldestTimeStampInMillis == 0L ? 0
      : (System.currentTimeMillis() - oldestTimeStampInMillis) / 1000);
    clear();
  }

  public synchronized void clear() {
    eventCounts.clear();
    bytesProcessed = 0;
    oldestTimeStampInMillis = 0;
  }

  public EventCounts getEventCounts() {
    return eventCounts;
  }

  public long getOldestTimeStampInMillis() {
    return oldestTimeStampInMillis;
  }

  public int getBytesProcessed() {
    return bytesProcessed;
  }
}
