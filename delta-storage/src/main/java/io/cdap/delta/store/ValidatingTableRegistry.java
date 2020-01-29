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
import io.cdap.delta.api.assessment.TableNotFoundException;
import io.cdap.delta.api.assessment.TableRegistry;

import java.io.IOException;
import java.util.List;

/**
 * Delegates to another TableRegistry, except it enforces the contract of the {@link #standardizeSchema(List)} method.
 */
public class ValidatingTableRegistry implements TableRegistry {
  private final TableRegistry delegate;

  public ValidatingTableRegistry(TableRegistry delegate) {
    this.delegate = delegate;
  }

  @Override
  public TableList listTables() throws IOException {
    return delegate.listTables();
  }

  @Override
  public SourceTableDetail describeTable(String database, String table) throws TableNotFoundException, IOException {
    return delegate.describeTable(database, table);
  }

  @Override
  public Schema standardizeSchema(List<ColumnDetail> tableDetail) {
    Schema schema = delegate.standardizeSchema(tableDetail);
    if (schema.getType() != Schema.Type.RECORD) {
      throw new IllegalStateException(
        String.format("Source plugin has a bug in schema standardization. "
                        + "It is returning a %s schema instead of a record schema.",
                      schema.getType().name()));
    }
    if (schema.getFields().size() != tableDetail.size()) {
      throw new IllegalStateException(
        String.format("Source plugin has a bug in schema standardization. "
                        + "The raw table has %d columns, but the standardized schema has %d columns.",
                      tableDetail.size(), schema.getFields().size()));
    }
    return schema;
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
