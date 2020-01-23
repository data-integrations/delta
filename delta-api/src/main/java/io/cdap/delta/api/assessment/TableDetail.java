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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Detailed information about a table.
 */
public class TableDetail extends TableSummary {
  private final List<String> primaryKey;
  private final List<ColumnDetail> columns;

  public TableDetail(String database, String table, List<String> primaryKey, List<ColumnDetail> columns) {
    super(database, table, columns.size());
    this.primaryKey = Collections.unmodifiableList(new ArrayList<>(primaryKey));
    this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
  }

  public List<String> getPrimaryKey() {
    return primaryKey;
  }

  public List<ColumnDetail> getColumns() {
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
    if (!super.equals(o)) {
      return false;
    }
    TableDetail that = (TableDetail) o;
    return Objects.equals(primaryKey, that.primaryKey) &&
      Objects.equals(columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), primaryKey, columns);
  }
}
