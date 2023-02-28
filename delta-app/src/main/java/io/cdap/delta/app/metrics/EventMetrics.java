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

import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.delta.api.DMLOperation;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Emits metrics about DDL and DML events.
 */
public class EventMetrics extends EventCounts{
  private final Metrics metrics;
  private AtomicLong oldestTimeStampInMillis;
  private AtomicInteger bytesProcessed;

  public EventMetrics(Metrics metrics) {
    this.metrics = metrics;
    this.oldestTimeStampInMillis = new AtomicLong();
    this.bytesProcessed = new AtomicInteger();
  }

  public void incrementDMLCount(DMLOperation op) {
    incrDmlEventCounts(op.getType());
    long oldestTimestamp = oldestTimeStampInMillis.get();
    if (oldestTimestamp == 0) {
      oldestTimeStampInMillis.set(op.getIngestTimestampMillis());
    } else {
      oldestTimeStampInMillis.set(Math.min(op.getIngestTimestampMillis(), oldestTimestamp));
    }
    bytesProcessed.addAndGet(op.getSizeInBytes());
  }

  public void emitDMLErrorMetric() {
    metrics.count("dml.errors", 1);
  }

  public void emitMetrics() {
    for (DMLOperation.Type op : dmlEventCounts.keySet()) {
      metrics.count(String.format("dml.%ss", op.name().toLowerCase()), getDMLCount(op));
    }
    metrics.count("ddl", getDDLCount());

    metrics.count("dml.data.processed.bytes", bytesProcessed.get());
    long olderstTimestamp = oldestTimeStampInMillis.get();
    metrics.gauge("dml.latency.seconds", olderstTimestamp == 0L ? 0
      : (System.currentTimeMillis() - olderstTimestamp) / 1000);
    clear();
  }

  public void clear() {
    super.clear();
    bytesProcessed.set(0);
    oldestTimeStampInMillis.set(0);
  }
}
