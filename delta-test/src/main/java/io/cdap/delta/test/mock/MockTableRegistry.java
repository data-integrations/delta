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

package io.cdap.delta.test.mock;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.assessment.StandardizedTableDetail;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableRegistry;

/**
 * Mock TableRegistry that just returns pre-defined objects.
 */
public class MockTableRegistry implements TableRegistry {
  private final TableList tableList;
  private final TableDetail tableDetail;
  private final Schema schema;

  public MockTableRegistry(TableList tableList, TableDetail tableDetail, Schema schema) {
    this.tableList = tableList;
    this.tableDetail = tableDetail;
    this.schema = schema;
  }

  @Override
  public TableList listTables() {
    return tableList;
  }

  @Override
  public TableDetail describeTable(String database, String table) {
    return tableDetail;
  }

  @Override
  public TableDetail describeTable(String database, String schema, String table) {
    return tableDetail;
  }

  @Override
  public StandardizedTableDetail standardize(TableDetail tableDetail) {
    return new StandardizedTableDetail(tableDetail.getDatabase(), tableDetail.getTable(),
                                       tableDetail.getPrimaryKey(), schema);
  }

  @Override
  public void close() {
    // no-op
  }
}
