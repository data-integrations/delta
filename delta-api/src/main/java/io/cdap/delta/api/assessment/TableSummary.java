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

package io.cdap.delta.api.assessment;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Summary of information about a table.
 */
public class TableSummary {
  private final String database;
  private final String table;
  private final int numColumns;
  // this schema is required for some db to uniquely identify the table, if this is not provided, it will not
  // be able to fetch the records
  private final String schema;

  public TableSummary(String database, String table, int numColumns, @Nullable String schema) {
    this.database = database;
    this.table = table;
    this.numColumns = numColumns;
    this.schema = schema;
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
  }

  public int getNumColumns() {
    return numColumns;
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
    TableSummary that = (TableSummary) o;
    return numColumns == that.numColumns &&
      Objects.equals(database, that.database) &&
      Objects.equals(table, that.table) &&
      Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, table, numColumns, schema);
  }
}
