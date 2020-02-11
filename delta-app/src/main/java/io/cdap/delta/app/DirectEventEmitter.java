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

import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.EventConsumer;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReaderDefinition;
import io.cdap.delta.api.Sequenced;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.proto.DBTable;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * In memory event emitter that simply passes the event directly to the delta target.
 * Also filters out events that should be ignored.
 */
public class DirectEventEmitter implements EventEmitter {
  private final EventConsumer consumer;
  private final DeltaContext context;
  private final Set<DMLOperation> dmlBlacklist;
  private final Set<DDLOperation> ddlBlacklist;
  private final Map<DBTable, SourceTable> tableDefinitions;
  private long sequenceNumber;

  public DirectEventEmitter(EventConsumer consumer, DeltaContext context, long sequenceNumber,
                            EventReaderDefinition readerDefinition) {
    this.consumer = consumer;
    this.context = context;
    this.sequenceNumber = sequenceNumber;
    this.dmlBlacklist = readerDefinition.getDmlBlacklist();
    this.ddlBlacklist = readerDefinition.getDdlBlacklist();
    this.tableDefinitions = readerDefinition.getTables().stream()
      .collect(Collectors.toMap(t -> new DBTable(t.getDatabase(), t.getTable()), t -> t));
  }

  @Override
  public void emit(DDLEvent event) {
    if (shouldIgnore(event)) {
      return;
    }

    sequenceNumber++;
    try {
      consumer.applyDDL(new Sequenced<>(event, sequenceNumber));
    } catch (Exception e) {
      // TODO: (CDAP-16251) retry
      throw new RuntimeException(e);
    }
  }

  @Override
  public void emit(DMLEvent event) {
    if (shouldIgnore(event)) {
      return;
    }

    sequenceNumber++;
    try {
      consumer.applyDML(new Sequenced<>(event, sequenceNumber));
    } catch (Exception e) {
      // TODO: (CDAP-16251) retry
      throw new RuntimeException(e);
    }
  }

  private boolean shouldIgnore(DDLEvent event) {
    if (ddlBlacklist.contains(event.getOperation())) {
      return true;
    }

    SourceTable sourceTable = tableDefinitions.get(new DBTable(event.getDatabase(), event.getTable()));
    return sourceTable != null && sourceTable.getDdlBlacklist().contains(event.getOperation());
  }

  private boolean shouldIgnore(DMLEvent event) {
    if (dmlBlacklist.contains(event.getOperation())) {
      return true;
    }

    SourceTable sourceTable = tableDefinitions.get(new DBTable(event.getDatabase(), event.getTable()));
    return sourceTable != null && sourceTable.getDmlBlacklist().contains(event.getOperation());
  }
}
