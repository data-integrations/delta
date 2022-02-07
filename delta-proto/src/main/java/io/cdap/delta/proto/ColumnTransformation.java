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

import java.util.Objects;

/**
 * Column level transformations
 */
public class ColumnTransformation {
  private String columnName;
  private String directive;

  public ColumnTransformation(String columnName, String directive) {
    this.columnName = columnName;
    this.directive = directive;
  }

  public String getColumnName() {
    return columnName;
  }

  public String getDirective() {
    return directive;
  }

  /**
   * Validate whether the columnName is non-empty and diretive is non-empty
   */
  public void validate() {
    if (columnName == null || columnName.isEmpty()) {
      throw new IllegalArgumentException(String.format("Column name of a ColumnTransformation for directive %s is " +
                                                         "null or empty.", directive));
    }
    if (directive == null || directive.isEmpty()) {
      throw new IllegalArgumentException(String.format("Directive of a ColumnTransformation for column %s is be null " +
                                                         "or empty.", columnName));
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
    ColumnTransformation that = (ColumnTransformation) o;
    return Objects.equals(columnName, that.columnName) &&
             Objects.equals(directive, that.directive);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnName, directive);
  }

  @Override
  public String toString() {
    return String.format("ColumnTransformation(columnName : \"%s\", directive : \"%s\")", columnName, directive);
  }
}

