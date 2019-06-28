/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.delta.mysql;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.delta.api.DeltaRuntimeContext;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.HistoryRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * History of DB changes.
 */
public class DBSchemaHistory extends AbstractDatabaseHistory {
  private static final String KEY = "history";
  // Hacky, fix when usage of EmbeddedEngine is replaced
  static DeltaRuntimeContext deltaRuntimeContext;
  private final DocumentWriter writer = DocumentWriter.defaultWriter();
  private final DocumentReader reader = DocumentReader.defaultReader();

  @Override
  protected synchronized void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
    List<HistoryRecord> history = getHistory();
    history.add(record);
    String historyStr = history.stream().map(r -> {
      try {
        return writer.write(r.document());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.joining("\n"));
    try {
      deltaRuntimeContext.putState(KEY, Bytes.toBytes(historyStr));
    } catch (IOException e) {
      // TODO: retry
    }
  }

  @Override
  protected synchronized void recoverRecords(Consumer<HistoryRecord> consumer) {
    for (HistoryRecord historyRecord : getHistory()) {
      consumer.accept(historyRecord);
    }
  }

  @Override
  public synchronized boolean exists() {
    try {
      return deltaRuntimeContext.getState(KEY) != null;
    } catch (IOException e) {
      return false;
    }
  }

  // TODO: cache history, should only have to read once
  private List<HistoryRecord> getHistory() {
    List<HistoryRecord> history = new ArrayList<>();
    try {
      byte[] historyBytes = deltaRuntimeContext.getState(KEY);
      if (historyBytes == null) {
        return history;
      }
      String historyStr = Bytes.toString(historyBytes);
      String[] historyRecords = historyStr.split("\n");
      for (String historyRecord : historyRecords) {
        history.add(new HistoryRecord(reader.read(historyRecord)));
      }
    } catch (IOException e) {
      // TODO: retry
    }
    return history;
  }
}
