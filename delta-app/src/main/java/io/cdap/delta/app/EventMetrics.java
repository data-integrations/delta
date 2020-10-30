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

import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.delta.api.DMLOperation;

import java.util.HashMap;
import java.util.Map;

/**
 * Emits metrics about DDL and DML events.
 */
public class EventMetrics {
  private final Metrics metrics;
  private final Map<DMLOperation.Type, Integer> dmlEventCounts;
  private int ddlEventCount;

  public EventMetrics(Metrics metrics) {
    this.metrics = metrics;
    this.dmlEventCounts = new HashMap<>();
    clear();
  }

  public synchronized void incrementDMLCount(DMLOperation.Type op) {
    dmlEventCounts.put(op, dmlEventCounts.get(op) + 1);
  }

  public synchronized void incrementDDLCount() {
    ddlEventCount++;
  }

  public synchronized void emitMetrics() {
    for (DMLOperation.Type op : dmlEventCounts.keySet()) {
      metrics.count(String.format("dml.%s", op.name().toLowerCase()), dmlEventCounts.get(op));
    }
    metrics.count("ddl", ddlEventCount);
    clear();
  }

  public synchronized void clear() {
    ddlEventCount = 0;
    for (DMLOperation.Type op : DMLOperation.Type.values()) {
      dmlEventCounts.put(op, 0);
    }
  }
}
