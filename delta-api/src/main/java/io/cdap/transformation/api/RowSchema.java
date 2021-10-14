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

import io.cdap.cdap.api.data.schema.Schema;

/**
 * Schema of a row. It's a mutable version of {@Schema}
 */
public interface RowSchema {
  /**
   * Get the {@link Schema.Field} of a column by its name.
   * if the column was once renamed , the new name should be used to get the {@link Schema.Field}.
   * An {@link IllegalArgumentException) will be thrown if the given column name doesn't exist.
   * @param columnName the name of the column. Once renamed , new name should be used to get the {@link Schema.Field}.
   * @return the {@link Schema.Field} of the corresponding column
   */
  Schema.Field getField(String columnName);

  /**
   * Set the {@link Schema.Field} of a column by its name in the {@link Schema.Field}. Once renamed, new name should be
   * used to set the {@link Schema.Field} again. If the given name doesn't exist , a new field will be added.
   * A {@link NullPointerException} will be thrown if the given column name is null.
   * @param field the new {@link Schema.Field} of the column
   */
  void setField(Schema.Field field);

  /**
   * Rename a field.
   * An {@link IllegalArgumentException) will be thrown if the original field name doesn't exist or the new field
   * name already exists
   *
   * @param originalName the original name of the field to be renamed.
   * @param newName      the new name of the field.
   */
  void renameField(String originalName, String newName);

  /**
   * @return the corresponding {@Schema} which is immutable
   */
  Schema toSchema();

}
