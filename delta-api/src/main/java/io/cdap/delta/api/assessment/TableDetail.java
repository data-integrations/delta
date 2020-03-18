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
import javax.annotation.Nullable;

/**
 * Detailed information about a table.
 */
public class TableDetail extends TableSummary {
  private final List<String> primaryKey;
  private final List<ColumnDetail> columns;
  private final List<Problem> features;

  private TableDetail(String database, String table, @Nullable String schema,
                      List<String> primaryKey, List<ColumnDetail> columns, List<Problem> features) {
    super(database, table, columns.size(), schema);
    this.primaryKey = Collections.unmodifiableList(new ArrayList<>(primaryKey));
    this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
    this.features = Collections.unmodifiableList(new ArrayList<>(features));
  }

  public List<String> getPrimaryKey() {
    return primaryKey;
  }

  public List<ColumnDetail> getColumns() {
    return columns;
  }

  public List<Problem> getFeatures() {
    return features;
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
      Objects.equals(columns, that.columns) &&
      Objects.equals(features, that.features);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), primaryKey, columns, features);
  }

  public static Builder builder(String database, String table, String schema) {
    return new Builder(database, table, schema);
  }

  /**
   * Builder for table detail.
   */
  public static class Builder {
    private String database;
    private String table;
    private String schema;
    private List<String> primaryKey = new ArrayList<>();
    private List<ColumnDetail> columns = new ArrayList<>();
    private List<Problem> features = new ArrayList<>();

    public Builder(String database, String table, String schema) {
      this.database = database;
      this.table = table;
      this.schema = schema;
    }

    public Builder setPrimaryKey(List<String> primaryKey) {
      this.primaryKey = primaryKey;
      return this;
    }

    public Builder setColumns(List<ColumnDetail> columns) {
      this.columns = columns;
      return this;
    }

    public Builder setFeatures(List<Problem> features) {
      this.features = features;
      return this;
    }

    public TableDetail build() {
      return new TableDetail(database, table, schema, primaryKey, columns, features);
    }
  }
}
