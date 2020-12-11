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

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents DDL operation.
 */
public class DDLOperation {
  /**
   * Type of DDL Operation
   */
  public enum Type {
    CREATE_DATABASE,
    DROP_DATABASE,
    CREATE_TABLE,
    DROP_TABLE,
    TRUNCATE_TABLE,
    ALTER_TABLE,
    RENAME_TABLE
  }

  private final String databaseName;
  private final String schemaName;
  private final String tableName;
  private final String prevTableName;
  private final DDLOperation.Type type;

  public DDLOperation(String databaseName, @Nullable String schemaName, @Nullable String tableName,
    DDLOperation.Type type) {
    this(databaseName, schemaName, tableName, null, type);
  }

  /**
   * Create a rename table DDL operation.
   * @param prevTableName previous name of the table
   * @param tableName new name of the table
   * @return rename table DDLOperation
   * @throws IllegalArgumentException when either the prevTableName or tableName is null
   */
  public static DDLOperation createRenameTableOperation(String databaseName, @Nullable String schemaName, String prevTableName,
    String tableName) {
    if (prevTableName == null || tableName == null) {
      throw new IllegalArgumentException("For table rename ddl operation both previous table name and " +
                                           "renamed table names are required.");
    }
    return new DDLOperation(databaseName, schemaName, prevTableName, tableName, Type.RENAME_TABLE);
  }

  private DDLOperation(String databaseName, @Nullable String schemaName, @Nullable String tableName,
    @Nullable String prevTableName,
    DDLOperation.Type type) {
    this.databaseName = databaseName;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.prevTableName = prevTableName;
    this.type = type;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  @Nullable
  public String getSchemaName() {
    return schemaName;
  }

  @Nullable
  public String getTableName() {
    return tableName;
  }

  @Nullable
  public String getPrevTableName() {
    return prevTableName;
  }

  public Type getType() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DDLOperation that = (DDLOperation) o;
    return Objects.equals(databaseName, that.databaseName) && Objects.equals(schemaName, that.schemaName) &&
      Objects.equals(tableName, that.tableName) && Objects.equals(prevTableName, that.prevTableName) &&
      type == that.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(databaseName, schemaName, tableName, prevTableName, type);
  }
}
