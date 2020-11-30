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

package io.cdap.delta.proto;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * A database and a table.
 */
public class DBTable {
  private final String database;
  private final String table;
  private final String schema;

  public DBTable(String database, String table) {
    this(database, null, table);
  }

  public DBTable(String database, @Nullable String schema, String table) {
    this.database = database;
    this.schema = schema;
    this.table = table;
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
  }

  @Nullable public String getSchema() {
    return schema;
  }

  /**
   * Validates that both database and table are non-null and non-empty. This is required when the object is created by
   * deserializing user provided input.
   */
  public void validate() {
    if (database == null || database.isEmpty()) {
      throw new IllegalArgumentException("The database is not specified. Please specify a database.");
    }
    if (table == null || table.isEmpty()) {
      throw new IllegalArgumentException("The table is not specified. Please specify a table.");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DBTable dbTable = (DBTable) o;
    return Objects.equals(database, dbTable.database) && Objects.equals(schema, dbTable.schema) &&
      Objects.equals(table, dbTable.table);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, schema, table);
  }
}
