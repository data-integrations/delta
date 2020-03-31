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
import javax.annotation.Nullable;

/**
 * A DDL change event.
 */
public class DDLEvent extends ChangeEvent {
  private final DDLOperation operation;
  private final Schema schema;
  private final String database;
  private final String prevTable;
  private final String table;
  private final List<String> primaryKey;

  private DDLEvent(Offset offset, DDLOperation operation, @Nullable Schema schema, String database,
                   @Nullable String prevTable, @Nullable String table, List<String> primaryKey, boolean isSnapshot) {
    super(offset, isSnapshot, ChangeType.DDL);
    this.operation = operation;
    this.schema = schema;
    this.database = database;
    this.prevTable = prevTable;
    this.table = table;
    this.primaryKey = Collections.unmodifiableList(new ArrayList<>(primaryKey));
  }

  public DDLOperation getOperation() {
    return operation;
  }

  @Nullable
  public Schema getSchema() {
    return schema;
  }

  public String getDatabase() {
    return database;
  }

  public String getPrevTable() {
    return prevTable;
  }

  @Nullable
  public String getTable() {
    return table;
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
    return operation == ddlEvent.operation &&
      Objects.equals(schema, ddlEvent.schema) &&
      Objects.equals(database, ddlEvent.database) &&
      Objects.equals(prevTable, ddlEvent.prevTable) &&
      Objects.equals(table, ddlEvent.table) &&
      Objects.equals(primaryKey, ddlEvent.primaryKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), operation, schema, database, prevTable, table, primaryKey);
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
    private DDLOperation operation;
    private Schema schema;
    private String database;
    private String prevTable;
    private String table;
    private List<String> primaryKey = new ArrayList<>();

    private Builder() { }

    private Builder(DDLEvent event) {
      this.offset = event.getOffset();
      this.isSnapshot = event.isSnapshot();
      this.operation = event.getOperation();
      this.schema = event.getSchema();
      this.database = event.getDatabase();
      this.prevTable = event.getPrevTable();
      this.table = event.getTable();
      this.primaryKey = event.getPrimaryKey();
    }

    public Builder setOperation(DDLOperation operation) {
      this.operation = operation;
      return this;
    }

    public Builder setSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    public Builder setDatabase(String database) {
      this.database = database;
      return this;
    }

    public Builder setPrevTable(String prevTable) {
      this.prevTable = prevTable;
      return this;
    }

    public Builder setTable(String table) {
      this.table = table;
      return this;
    }

    public Builder setPrimaryKey(List<String> primaryKey) {
      this.primaryKey = new ArrayList<>(primaryKey);
      return this;
    }

    public DDLEvent build() {
      return new DDLEvent(offset, operation, schema, database, prevTable, table, primaryKey, isSnapshot);
    }
  }
}
