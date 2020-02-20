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
 * Uniquely identifies a table.
 */
public class TableId {
  private final String database;
  private final String table;
  // this schema is required for some db to uniquely identify the table, if this is not provided, it will not
  // be able to fetch the records
  // TODO: CDAP-16261 have a better way to identify the table
  private final String schema;

  public TableId(String database, String table, @Nullable String schema) {
    this.database = database;
    this.table = table;
    this.schema = schema;
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
  }

  @Nullable
  public String getSchema() {
    return schema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableId tableId = (TableId) o;
    return Objects.equals(database, tableId.database) &&
      Objects.equals(table, tableId.table) &&
      Objects.equals(schema, tableId.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, table, schema);
  }
}
