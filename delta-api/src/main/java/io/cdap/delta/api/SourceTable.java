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
import java.util.List;
import java.util.Objects;

/**
 * Object representing what data from a table should be read by the source.
 */
public class SourceTable {
  private final String database;
  private final String table;
  private final List<SourceColumn> columns;

  public SourceTable(String database, String table) {
    this(database, table, Collections.emptyList());
  }

  public SourceTable(String database, String table, List<SourceColumn> columns) {
    this.database = database;
    this.table = table;
    this.columns = columns;
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
  }

  public List<SourceColumn> getColumns() {
    return columns;
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
      Objects.equals(columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, table, columns);
  }
}
