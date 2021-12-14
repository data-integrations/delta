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

package io.cdap.delta.store;

import com.google.gson.Gson;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.FieldType;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.delta.app.DeltaWorkerId;
import io.cdap.delta.app.OffsetAndSequence;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * A StructuredTable based database table to store replication offset and sequence data.
 */
public class DBReplicationOffsetStore {

  private static final StructuredTableId TABLE_ID = new StructuredTableId("delta_offset_store");
  private static final String NAMESPACE_COL = "namespace";
  private static final String APP_GENERATION_COL = "app_generation";
  private static final String APP_NAME_COL = "app_name";
  private static final String INSTANCE_ID_COL = "instance_id";
  private static final String OFFSET_COL = "offset_sequence";
  private static final String UPDATED_COL = "last_updated";

  public static final StructuredTableSpecification TABLE_SPEC = new StructuredTableSpecification.Builder()
    .withId(TABLE_ID)
    .withFields(new FieldType(NAMESPACE_COL, FieldType.Type.STRING),
                new FieldType(APP_GENERATION_COL, FieldType.Type.LONG),
                new FieldType(APP_NAME_COL, FieldType.Type.STRING),
                new FieldType(INSTANCE_ID_COL, FieldType.Type.INTEGER),
                new FieldType(OFFSET_COL, FieldType.Type.STRING),
                new FieldType(UPDATED_COL, FieldType.Type.LONG))
    .withPrimaryKeys(NAMESPACE_COL, APP_NAME_COL, APP_GENERATION_COL, INSTANCE_ID_COL)
    .build();

  private final StructuredTable table;
  private static final Gson GSON = new Gson();

  private DBReplicationOffsetStore(StructuredTable table) {
    this.table = table;
  }

  static DBReplicationOffsetStore get(StructuredTableContext context) {
    try {
      StructuredTable table = context.getTable(TABLE_ID);
      return new DBReplicationOffsetStore(table);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(String.format(
        "System table '%s' does not exist. Please check your system environment.", TABLE_ID.getName()), e);
    }
  }

  @Nullable
  public OffsetAndSequence getOffsets(DeltaWorkerId id)
    throws IOException {
    List<Field<?>> keys = getKey(id);
    Optional<StructuredRow> row = table.read(keys);
    if (!row.isPresent() || row.get().getString(OFFSET_COL) == null) {
      return null;
    }
    String offsetStrJson = row.get().getString(OFFSET_COL);
    return GSON.fromJson(offsetStrJson, OffsetAndSequence.class);
  }

  public void writeOffset(DeltaWorkerId id, OffsetAndSequence data)
    throws IOException {
    Collection<Field<?>> fields = getKey(id);
    fields.add(Fields.stringField(OFFSET_COL, GSON.toJson(data)));
    fields.add(Fields.longField(UPDATED_COL, System.currentTimeMillis()));

    table.upsert(fields);
  }

  private List<Field<?>> getKey(DeltaWorkerId id) {

    List<Field<?>> keyFields = new ArrayList<>(4);
    keyFields.add(Fields.stringField(NAMESPACE_COL, id.getPipelineId().getNamespace()));
    keyFields.add(Fields.stringField(APP_NAME_COL, id.getPipelineId().getApp()));
    keyFields.add(Fields.longField(APP_GENERATION_COL, id.getPipelineId().getGeneration()));
    keyFields.add(Fields.intField(INSTANCE_ID_COL, id.getInstanceId()));
    return keyFields;
  }
}
