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

import io.cdap.delta.api.ChangeEvent;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReaderDefinition;
import io.cdap.delta.api.Sequenced;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.proto.DBTable;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

/**
 * An event emitter that enqueues change events in an in-memory queue.
 * Also filters out events that should be ignored.
 *
 * If emitting an event is interrupted, all subsequent emit calls will be no-ops.
 */
public class QueueingEventEmitter implements EventEmitter {
  private final Set<DMLOperation.Type> dmlBlacklist;
  private final Set<DDLOperation.Type> ddlBlacklist;
  private final Map<DBTable, SourceTable> tableDefinitions;
  private final BlockingQueue<Sequenced<? extends ChangeEvent>> eventQueue;
  private long sequenceNumber;
  private volatile boolean interrupted;

  QueueingEventEmitter(EventReaderDefinition readerDefinition, long sequenceNumber,
                       BlockingQueue<Sequenced<? extends ChangeEvent>> eventQueue) {
    this.sequenceNumber = sequenceNumber;
    this.dmlBlacklist = readerDefinition.getDmlBlacklist();
    this.ddlBlacklist = readerDefinition.getDdlBlacklist();
    this.tableDefinitions = readerDefinition.getTables().stream()
      .collect(Collectors.toMap(t -> new DBTable(t.getDatabase(), t.getTable()), t -> t));
    this.eventQueue = eventQueue;
    this.interrupted = false;
  }

  @Override
  public void emit(DDLEvent event) {
    if (interrupted || shouldIgnore(event)) {
      return;
    }

    try {
      // don't assign sequence number to DDL event
      eventQueue.put(new Sequenced<>(event));
    } catch (InterruptedException e) {
      // this should only happen when the event consumer is stopped
      // in that case, don't emit any more events
      interrupted = true;
    }
  }

  @Override
  public void emit(DMLEvent event) {
    if (interrupted || shouldIgnore(event)) {
      return;
    }

    try {
      eventQueue.put(new Sequenced<>(event, ++sequenceNumber));
    } catch (InterruptedException e) {
      // this should only happen when the event consumer is stopped
      // in that case, don't emit any more events
      interrupted = true;
    }
  }

  private boolean shouldIgnore(DDLEvent event) {
    DDLOperation.Type ddlOperationType = event.getOperation().getType();
    if (ddlBlacklist.contains(ddlOperationType)) {
      return true;
    } else if (ddlOperationType == DDLOperation.Type.CREATE_DATABASE ||
      ddlOperationType == DDLOperation.Type.DROP_DATABASE) {
      return false;
    }

    SourceTable sourceTable = tableDefinitions.get(new DBTable(event.getOperation().getDatabaseName(),
                                                               event.getOperation().getTableName()));
    if (sourceTable != null && sourceTable.getDdlBlacklist().contains(ddlOperationType)) {
      return true;
    }

    return !tableDefinitions.isEmpty() && sourceTable == null;
  }

  private boolean shouldIgnore(DMLEvent event) {
    if (dmlBlacklist.contains(event.getOperation().getType())) {
      return true;
    }

    SourceTable sourceTable =
      tableDefinitions.get(new DBTable(event.getOperation().getDatabaseName(), event.getOperation().getTableName()));
    if (sourceTable != null && sourceTable.getDmlBlacklist().contains(event.getOperation().getType())) {
      return true;
    }
    return !tableDefinitions.isEmpty() && sourceTable == null;
  }
}
