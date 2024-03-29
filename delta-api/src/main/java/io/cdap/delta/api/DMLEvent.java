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

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;

/**
 * A DML change event.
 */
public class DMLEvent extends ChangeEvent {
  private final DMLOperation operation;
  private final StructuredRecord row;
  private final StructuredRecord previousRow;
  private final String transactionId;
  private final long ingestTimestampMillis;
  private final String rowId;
  private int sizeInBytes;
  private List<SortKey> sortKeys;

  private DMLEvent(Offset offset, DMLOperation operation, StructuredRecord row,
                   @Nullable StructuredRecord previousRow, @Nullable String transactionId, long ingestTimestampMillis,
                   @Nullable Long sourceTimestampMillis, boolean isSnapshot, @Nullable String rowId,
                   @Nullable List<SortKey> sortKeys) {
    super(offset, isSnapshot, ChangeType.DML, sourceTimestampMillis);
    this.operation = operation;
    this.row = row;
    this.previousRow = previousRow;
    this.transactionId = transactionId;
    this.ingestTimestampMillis = ingestTimestampMillis;
    this.rowId = rowId;
    this.sizeInBytes = -1;
    this.sortKeys = sortKeys;
  }

  public DMLOperation getOperation() {
    return operation;
  }

  public StructuredRecord getRow() {
    return row;
  }

  /**
   * @return previous value of the row. This will be set for update events and null for inserts and deletes.
   */
  @Nullable
  public StructuredRecord getPreviousRow() {
    return previousRow;
  }

  public String getTransactionId() {
    return transactionId;
  }

  public long getIngestTimestampMillis() {
    return ingestTimestampMillis;
  }

  @Nullable
  public String getRowId() {
    return rowId;
  }

  @Nullable
  public List<SortKey> getSortKeys() {
    return sortKeys;
  }

  public int getSizeInBytes() {
    if (sizeInBytes < 0) {
      // the size of all other fields are fixed, only return the dynamic size of row and previousRow
      sizeInBytes = operation.getSizeInBytes() + computeSizeInBytes(previousRow);
      // delete event didn't count the size of row in the DMLOperation
      if (operation.getType() == DMLOperation.Type.DELETE) {
        sizeInBytes += computeSizeInBytes(row);
      }
    }
    return sizeInBytes;
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
      Objects.equals(rowId, dmlEvent.rowId) &&
      Objects.equals(operation, dmlEvent.operation) &&
      Objects.equals(row, dmlEvent.row) &&
      Objects.equals(previousRow, dmlEvent.previousRow) &&
      Objects.equals(transactionId, dmlEvent.transactionId) &&
      Objects.equals(sortKeys, dmlEvent.sortKeys);
  }

  @Override
  public String toString() {
    StringJoiner stringJoiner = new StringJoiner(", ", "{", "}");
    stringJoiner.add("operation=" + operation);
    stringJoiner.add("isSnapshot=" + this.isSnapshot());
    stringJoiner.add("changeType=" + this.getChangeType());
    stringJoiner.add("offset=" + this.getOffset());
    if (this.getSourceTimestampMillis() != null) {
      stringJoiner.add("sourceTimestampMillis=" + this.getSourceTimestampMillis());
    }
    if (sortKeys != null && !sortKeys.isEmpty()) {
      stringJoiner.add("sortKeys=" + sortKeys);
    }
    return stringJoiner.toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), operation, row, previousRow, transactionId, ingestTimestampMillis,
                        rowId, sortKeys);
  }

  public static Builder builder(DMLEvent event) {
    return new Builder(event);
  }

  public static Builder builder() {
    return new Builder();
  }

  private static int computeSizeInBytes(StructuredRecord row) {
    if (row == null) {
      return 0;
    }
    List<Schema.Field> fields = row.getSchema().getFields();
    if (fields == null) {
      return 0;
    }
    int sizeInBytes = 0;
    for (Schema.Field field : fields) {
      Schema fieldSchema = field.getSchema();
      fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
      Object value = row.get(field.getName());
      if (value == null) {
        continue;
      }
      switch (fieldSchema.getType()) {
        case BOOLEAN:
          sizeInBytes += 1;
          break;
        case INT:
        case FLOAT:
        case ENUM:
          sizeInBytes += 4;
          break;
        case LONG:
        case DOUBLE:
          sizeInBytes += 8;
          break;
        case BYTES:
          if (value instanceof ByteBuffer) {
            sizeInBytes += Bytes.toBytes((ByteBuffer) value).length;
          } else {
            sizeInBytes += ((byte[]) value).length;
          }
          break;
        case STRING:
          sizeInBytes += ((String) value).getBytes(StandardCharsets.UTF_8).length;
          break;
        default:
          break;
      }
    }
    return sizeInBytes;
  }

  /**
   * Builder for a DML event.
   */
  public static class Builder extends ChangeEvent.Builder<Builder> {
    private DMLOperation.Type operationType;
    private String databaseName;
    private String schemaName;
    private String tableName;
    private StructuredRecord row;
    private StructuredRecord previousRow;
    private String transactionId;
    private long ingestTimestampMillis;
    private String rowId;
    private List<SortKey> sortKeys;

    private Builder() { }

    private Builder(DMLEvent event) {
      this.offset = event.getOffset();
      this.operationType = event.getOperation().getType();
      this.databaseName = event.getOperation().getDatabaseName();
      this.schemaName = event.getOperation().getSchemaName();
      this.tableName = event.getOperation().getTableName();
      this.row = event.getRow();
      this.previousRow = event.getPreviousRow();
      this.transactionId = event.getTransactionId();
      this.ingestTimestampMillis = event.getIngestTimestampMillis();
      this.isSnapshot = event.isSnapshot();
      this.sourceTimestampMillis = event.getSourceTimestampMillis();
      this.rowId = event.getRowId();
      this.sortKeys = event.getSortKeys();
    }

    public Builder setOperationType(DMLOperation.Type operationType) {
      this.operationType = operationType;
      return this;
    }

    public Builder setDatabaseName(String databaseName) {
      this.databaseName = databaseName;
      return this;
    }

    public Builder setSchemaName(String schemaName) {
      this.schemaName = schemaName;
      return this;
    }

    public Builder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder setRow(StructuredRecord row) {
      this.row = row;
      return this;
    }

    public Builder setPreviousRow(StructuredRecord previousRow) {
      this.previousRow = previousRow;
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

    public Builder setRowId(String rowId) {
      this.rowId = rowId;
      return this;
    }

    public Builder setSortKeys(List<SortKey> sortKeys) {
      this.sortKeys = sortKeys;
      return this;
    }

    public DMLEvent build() {
      int sizeInBytes = (operationType == DMLOperation.Type.INSERT ||
        operationType == DMLOperation.Type.UPDATE) ? computeSizeInBytes(row) : 0;

      return new DMLEvent(offset,
        new DMLOperation(databaseName, schemaName, tableName, operationType, ingestTimestampMillis, sizeInBytes), row,
        previousRow, transactionId, ingestTimestampMillis, sourceTimestampMillis, isSnapshot, rowId, sortKeys);
    }

  }
}
