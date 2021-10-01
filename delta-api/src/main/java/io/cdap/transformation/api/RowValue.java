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

package io.cdap.transformation.api;

/**
 * Value of a row.
 */
public interface RowValue {
  /**
   * Get the value of a column by its name.
   * if the column was once renamed , the new name should be used to get the value.
   * An {@link IllegalArgumentException) will be thrown if the given column name doesn't exist.
   * @param columnName the name of the column. Once column was renamed , new name should be used to get the value.
   * @return the value of the corresponding column
   */
  Object getColumnValue(String columnName);

  /**
   * Set the value of a column by its name. Once column was renamed, new name should be used to
   * set the value. If the given column doesn't exist , a new column will be added.
   * A {@link NullPointerException} will be thrown if the given column name is null.
   * @param columnName the latest name of a column
   * @param value the value of the column
   */
  void setColumnValue(String columnName, Object value);

  /**
   * Rename a column.
   * An {@link IllegalArgumentException) will be thrown if the original column name doesn't exist or the new column
   * name already exists
   *
   * @param originalName the original name of the column to be renamed.
   * @param newName      the new name of the column.
   * @return the value of the removed column.
   */
  Object renameColumn(String originalName, String newName);
}
