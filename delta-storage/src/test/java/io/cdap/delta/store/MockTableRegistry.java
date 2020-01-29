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

package io.cdap.delta.store;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.SourceTableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableRegistry;

import java.util.List;

/**
 * Mock TableRegistry that just returns pre-defined objects.
 */
public class MockTableRegistry implements TableRegistry {
  private final TableList tableList;
  private final SourceTableDetail tableDetail;
  private final Schema schema;

  public MockTableRegistry(TableList tableList, SourceTableDetail tableDetail, Schema schema) {
    this.tableList = tableList;
    this.tableDetail = tableDetail;
    this.schema = schema;
  }

  @Override
  public TableList listTables() {
    return tableList;
  }

  @Override
  public SourceTableDetail describeTable(String database, String table) {
    return tableDetail;
  }

  @Override
  public Schema standardizeSchema(List<ColumnDetail> tableDetail) {
    return schema;
  }

  @Override
  public void close() {
    // no-op
  }
}
