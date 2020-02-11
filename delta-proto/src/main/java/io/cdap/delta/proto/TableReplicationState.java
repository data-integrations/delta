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

package io.cdap.delta.proto;

import io.cdap.delta.api.ReplicationError;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * State of replication for a table.
 */
public class TableReplicationState {
  private final String database;
  private final String table;
  private final TableState state;
  private final ReplicationError error;

  public TableReplicationState(String database, String table, TableState state, @Nullable ReplicationError error) {
    this.database = database;
    this.table = table;
    this.state = state;
    this.error = error;
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
  }

  public TableState getState() {
    return state;
  }

  public ReplicationError getError() {
    return error;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableReplicationState that = (TableReplicationState) o;
    return Objects.equals(database, that.database) &&
      Objects.equals(table, that.table) &&
      state == that.state &&
      Objects.equals(error, that.error);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, table, state, error);
  }
}
