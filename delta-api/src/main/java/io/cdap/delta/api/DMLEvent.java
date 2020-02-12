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

package io.cdap.delta.api;

import io.cdap.cdap.api.data.format.StructuredRecord;

import java.util.Objects;
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

  private DMLEvent(Offset offset, DMLOperation operation, String database, String table, StructuredRecord row,
                   @Nullable String transactionId, long ingestTimestampMillis, boolean isSnapshot) {
    super(offset, isSnapshot, ChangeType.DML);
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    DMLEvent dmlEvent = (DMLEvent) o;
    return ingestTimestampMillis == dmlEvent.ingestTimestampMillis &&
      operation == dmlEvent.operation &&
      Objects.equals(database, dmlEvent.database) &&
      Objects.equals(table, dmlEvent.table) &&
      Objects.equals(row, dmlEvent.row) &&
      Objects.equals(transactionId, dmlEvent.transactionId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), operation, database, table, row, transactionId, ingestTimestampMillis);
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

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for a DML event.
   */
  public static class Builder extends ChangeEvent.Builder<Builder> {
    private DMLOperation operation;
    private String database;
    private String table;
    private StructuredRecord row;
    private String transactionId;
    private long ingestTimestampMillis;

    public Builder setOperation(DMLOperation operation) {
      this.operation = operation;
      return this;
    }

    public Builder setDatabase(String database) {
      this.database = database;
      return this;
    }

    public Builder setTable(String table) {
      this.table = table;
      return this;
    }

    public Builder setRow(StructuredRecord row) {
      this.row = row;
      return this;
    }

    public Builder setTransactionId(String transactionId) {
      this.transactionId = transactionId;
      return this;
    }

    public Builder setIngestTimestamp(long ingestTimestampMillis) {
      this.ingestTimestampMillis = ingestTimestampMillis;
      return this;
    }

    public DMLEvent build() {
      return new DMLEvent(offset, operation, database, table, row, transactionId, ingestTimestampMillis, isSnapshot);
    }
  }
}
