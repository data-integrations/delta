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

import io.cdap.cdap.api.data.schema.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * A DDL change event.
 */
public class DDLEvent extends ChangeEvent {
  private final DDLOperation operation;
  private final Schema schema;
  private final List<String> primaryKey;

  private DDLEvent(Offset offset, DDLOperation operation, @Nullable Schema schema,
                   List<String> primaryKey, boolean isSnapshot, @Nullable Long sourceTimestampMillis) {
    super(offset, isSnapshot, ChangeType.DDL, sourceTimestampMillis);
    this.operation = operation;
    this.schema = schema;
    this.primaryKey = Collections.unmodifiableList(new ArrayList<>(primaryKey));
  }

  public DDLOperation getOperation() {
    return operation;
  }

  @Nullable
  public Schema getSchema() {
    return schema;
  }

  public List<String> getPrimaryKey() {
    return primaryKey;
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
    DDLEvent ddlEvent = (DDLEvent) o;
    return Objects.equals(operation, ddlEvent.operation) &&
      Objects.equals(schema, ddlEvent.schema) &&
      Objects.equals(primaryKey, ddlEvent.primaryKey);
  }
  @Override
  public String toString() {
    String operationString = "operation=" + operation;
    String isSnapshotString = "isSnapshot=" + this.isSnapshot();
    String changeTypeString = "changeType=" + this.getChangeType();
    String primaryKeyString = primaryKey == null || primaryKey.isEmpty() ?
                              null : "primaryKey=" + "[" + String.join(",", primaryKey) + "]";
    String offsetString = this.getOffset().get() != null ? "offset=" + this.getOffset().get() : null;
    String sourceTimeString  = this.getSourceTimestampMillis() != null ? "sourceTimestampMillis=" +
                              this.getSourceTimestampMillis() : null;
    String ddlEvent = Stream.of(changeTypeString, operationString, primaryKeyString, offsetString,
                                    isSnapshotString, sourceTimeString)
                            .filter(s -> s != null && !s.isEmpty())
                            .collect(Collectors.joining(", "));
    return "{" + ddlEvent + "}";
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), operation, schema, primaryKey);
  }

  public static Builder builder(DDLEvent event) {
    return new Builder(event);
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for a DDL event.
   */
  public static class Builder extends ChangeEvent.Builder<Builder> {
    private DDLOperation.Type operation;
    private Schema schema;
    private String databaseName;
    private String schemaName;
    private String prevTableName;
    private String tableName;
    private List<String> primaryKey = new ArrayList<>();

    private Builder() { }

    private Builder(DDLEvent event) {
      this.offset = event.getOffset();
      this.isSnapshot = event.isSnapshot();
      this.operation = event.getOperation().getType();
      this.schema = event.getSchema();
      this.schemaName = event.getOperation().getSchemaName();
      this.databaseName = event.getOperation().getDatabaseName();
      this.prevTableName = event.getOperation().getPrevTableName();
      this.tableName = event.getOperation().getTableName();
      this.primaryKey = event.getPrimaryKey();
    }

    public Builder setOperation(DDLOperation.Type operation) {
      this.operation = operation;
      return this;
    }

    public Builder setSchema(Schema schema) {
      this.schema = schema;
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

    public Builder setPrevTableName(String prevTableName) {
      this.prevTableName = prevTableName;
      return this;
    }

    public Builder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder setPrimaryKey(List<String> primaryKey) {
      this.primaryKey = new ArrayList<>(primaryKey);
      return this;
    }

    public DDLEvent build() {
      DDLOperation ddlOperation;
      if (operation == DDLOperation.Type.RENAME_TABLE) {
        ddlOperation = DDLOperation.createRenameTableOperation(databaseName, schemaName, prevTableName, tableName);
      } else {
        ddlOperation = new DDLOperation(databaseName, schemaName, tableName, operation);
      }
      return new DDLEvent(offset, ddlOperation, schema, primaryKey, isSnapshot, sourceTimestampMillis);
    }
  }
}
