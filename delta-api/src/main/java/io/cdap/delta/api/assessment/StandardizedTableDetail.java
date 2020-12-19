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

import io.cdap.cdap.api.data.schema.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Standardized information about a source table. This contains the schema of data that the target will see after the
 * source has converted its native types into standard CDAP StructuredRecords.
 */
public class StandardizedTableDetail {
  private final String database;
  private final String table;
  private final String schemaName;
  private final List<String> primaryKey;
  private final Schema schema;

  public StandardizedTableDetail(String database, @Nullable String schemaName, String table, List<String> primaryKey,
    Schema schema) {
    this.database = database;
    this.schemaName = schemaName;
    this.table = table;
    this.primaryKey = Collections.unmodifiableList(new ArrayList<>(primaryKey));
    this.schema = schema;
  }

  public StandardizedTableDetail(String database, String table, List<String> primaryKey, Schema schema) {
    this(database, null, table, primaryKey, schema);
  }

  public String getDatabase() {
    return database;
  }

  @Nullable
  public String getSchemaName() {
    return schemaName;
  }

  public String getTable() {
    return table;
  }

  public List<String> getPrimaryKey() {
    return primaryKey;
  }

  public Schema getSchema() {
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
    StandardizedTableDetail that = (StandardizedTableDetail) o;
    return Objects.equals(database, that.database) && Objects.equals(schemaName, that.schemaName) &&
      Objects.equals(table, that.table) && Objects.equals(primaryKey, that.primaryKey) &&
      Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, schemaName, table, primaryKey, schema);
  }
}
