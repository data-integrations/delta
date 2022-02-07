/*
 * Copyright © 2021 Cask Data, Inc.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Table level transformation which contains the information of transformations for a specific table
 */
public class TableTransformation {
  private String tableName;
  private List<ColumnTransformation> columnTransformations;

  public TableTransformation(String tableName, List<ColumnTransformation> columnTransformations) {
    this.tableName = tableName;
    this.columnTransformations = Collections.unmodifiableList(new ArrayList<>(columnTransformations));
  }

  public String getTableName() {
    return tableName;
  }

  /**
   * @return an ordered list of column level transformations. Those transformations will be applied in returned order.
   */
  public List<ColumnTransformation> getColumnTransformations() {
    return columnTransformations == null ? Collections.emptyList() : columnTransformations;
  }

  /**
   * Validate whether table name is non-empty and column transformations are valid.
   */
  public void validate() {
    if (tableName == null || tableName.isEmpty()) {
      throw new IllegalArgumentException(String.format("Table name of a TableTransformation that contains below " +
                                                         "ColumnTransformations : %s is null or empty.",
                                                       columnTransformations));
    }
    if (columnTransformations != null) {
      for (ColumnTransformation columnTransformation : columnTransformations) {
        columnTransformation.validate();
      }
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
    TableTransformation that = (TableTransformation) o;
    return Objects.equals(tableName, that.tableName) &&
             Objects.equals(columnTransformations, that.columnTransformations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, columnTransformations);
  }
}
