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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Object representing what data from a table should be read by the source.
 */
public class SourceTable {
  private final String database;
  private final String table;
  // this schema is required for some db to uniquely identify the table, if this is not provided, it will not
  // be able to fetch the records
  // TODO: CDAP-16261 have a better way to identify the table
  private final String schema;
  private final Set<SourceColumn> columns;
  private final Set<DMLOperation> dmlBlacklist;
  private final Set<DDLOperation> ddlBlacklist;

  public SourceTable(String database, String table) {
    this(database, table, Collections.emptySet());
  }

  public SourceTable(String database, String table, Set<SourceColumn> columns) {
    this(database, table, null, columns, Collections.emptySet(), Collections.emptySet());
  }

  public SourceTable(String database, String table, @Nullable String schema,
                     Set<SourceColumn> columns, Set<DMLOperation> dmlBlacklist,
                     Set<DDLOperation> ddlBlacklist) {
    this.database = database;
    this.table = table;
    this.schema = schema;
    this.columns = columns;
    this.dmlBlacklist = new HashSet<>(dmlBlacklist);
    this.ddlBlacklist = new HashSet<>(ddlBlacklist);
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
  }

  /**
   * @return table columns that should be replicated. If empty, all columns should be replicated.
   */
  public Set<SourceColumn> getColumns() {
    // check for null because this object can be created through deserialization of user provided input
    return columns == null ? Collections.emptySet() : Collections.unmodifiableSet(columns);
  }

  /**
   * @return set of DML operations that should be ignored
   */
  public Set<DMLOperation> getDmlBlacklist() {
    return dmlBlacklist == null ? Collections.emptySet() : Collections.unmodifiableSet(dmlBlacklist);
  }

  @Nullable
  public String getSchema() {
    return schema;
  }

  /**
   * @return set of DDL operations that should be ignored
   */
  public Set<DDLOperation> getDdlBlacklist() {
    return ddlBlacklist == null ? Collections.emptySet() : Collections.unmodifiableSet(ddlBlacklist);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SourceTable that = (SourceTable) o;
    return Objects.equals(database, that.database) &&
      Objects.equals(table, that.table) &&
      Objects.equals(columns, that.columns) &&
      Objects.equals(dmlBlacklist, that.dmlBlacklist) &&
      Objects.equals(ddlBlacklist, that.ddlBlacklist) &&
      Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, table, columns, dmlBlacklist, ddlBlacklist, schema);
  }
}
