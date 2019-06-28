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

package io.cdap.delta.api;

import io.cdap.cdap.api.data.format.StructuredRecord;

import javax.annotation.Nullable;

/**
 * A DML change event.
 */
public class DMLEvent extends ChangeEvent {
  private final DMLOperation operation;
  private final String database;
  private final String table;
  private final StructuredRecord row;
  private final String transactionId;
  private final long ingestTimestampMillis;

  public DMLEvent(Offset offset, DMLOperation operation, String database, String table, StructuredRecord row,
                  @Nullable String transactionId, long ingestTimestampMillis) {
    super(offset);
    this.operation = operation;
    this.database = database;
    this.table = table;
    this.row = row;
    this.transactionId = transactionId;
    this.ingestTimestampMillis = ingestTimestampMillis;
  }

  public DMLOperation getOperation() {
    return operation;
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
  }

  public StructuredRecord getRow() {
    return row;
  }

  public String getTransactionId() {
    return transactionId;
  }

  public long getIngestTimestampMillis() {
    return ingestTimestampMillis;
  }

  @Override
  public String toString() {
    return "DMLEvent{" +
      "operation=" + operation +
      ", table='" + table + '\'' +
      ", row=" + row +
      ", transactionId='" + transactionId + '\'' +
      ", ingestTimestampMillis=" + ingestTimestampMillis +
      '}';
  }
}
