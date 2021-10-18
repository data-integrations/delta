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
 * Mutable vale of a row where you can set or get column value of a row via the column name. Also you can rename a
 * column.
 */
public interface MutableRowValue {
  /**
   * Get the value of a column by its name.
   * if the column was once renamed , the new name should be used to get the value.
   * @param columnName the name of the column. Once column was renamed, new name should be used to get the value.
   * @return the value of the corresponding column.
   * @throws NotFoundException if the given column name doesn't exist.
   */
  Object getColumnValue(String columnName) throws NotFoundException;

  /**
   * Set the value of a column by its name. Once column was renamed, new name should be used to
   * set the value. If the given column doesn't exist , a new column will be added.
   * @param columnName the latest name of a column.
   * @param value the value of the column.
   * @throws IllegalArgumentException  if the given column name is null.
   */
  void setColumnValue(String columnName, Object value);

  /**
   * Rename a column.
   *
   * @param originalName the original name of the column to be renamed.
   * @param newName      the new name of the column.
   * @throws IllegalArgumentException will be thrown if the original column name doesn't exist or the new column name
   * already exists.
   */
  void renameColumn(String originalName, String newName);
}
