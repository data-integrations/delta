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

import io.cdap.delta.api.DMLOperation;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Stores counts for DDL/DML events
 *
 * Instance can be reused by calling clear
 */
public class EventCounts {
  private static final AtomicInteger ZERO = new AtomicInteger(0);
  private Map<DMLOperation.Type, AtomicInteger> dmlEventCounts;
  private AtomicInteger ddlEventCount;

  public EventCounts() {
    ddlEventCount = new AtomicInteger();
    dmlEventCounts = new EnumMap<>(DMLOperation.Type.class);
    for (DMLOperation.Type op : DMLOperation.Type.values()) {
      dmlEventCounts.put(op, new AtomicInteger(0));
    }
  }

  public Map<DMLOperation.Type, AtomicInteger> getDmlEventCounts() {
    return dmlEventCounts;
  }

  public void incrementDDLCount() {
    ddlEventCount.incrementAndGet();
  }

  public void incrementDDLCount(int count) {
    ddlEventCount.addAndGet(count);
  }

  public void incrementDMLCount(DMLOperation.Type type) {
    dmlEventCounts.get(type).incrementAndGet();
  }

  public void incrementDMLCount(DMLOperation.Type type, int count) {
    dmlEventCounts.get(type).addAndGet(count);
  }

  public int getDdlEventCount() {
    return ddlEventCount.get();
  }

  public int getDMLCount(DMLOperation.Type type) {
    return dmlEventCounts.getOrDefault(type, ZERO).get();
  }

  public void clear() {
    ddlEventCount.set(0);
    for (DMLOperation.Type op : DMLOperation.Type.values()) {
      dmlEventCounts.get(op).set(0);
    }
  }
}
