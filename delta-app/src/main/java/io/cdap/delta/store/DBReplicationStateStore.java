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

import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.FieldType;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.delta.app.DeltaWorkerId;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

/**
 * A StructuredTable based database table to store generic replicator states.
 */
public class DBReplicationStateStore {

  private static final StructuredTableId TABLE_ID = new StructuredTableId("delta_state_store");
  private static final String NAMESPACE_COL = "namespace";
  private static final String APP_GENERATION_COL = "app_generation";
  private static final String APPNAME_COL = "app_name";
  private static final String INSTANCEID_COL = "instance_id";
  private static final String STATE_COL = "state";
  private static final String VALUE_COL = "value";
  private static final String UPDATED_COL = "last_updated";

  public static final StructuredTableSpecification TABLE_SPEC = new StructuredTableSpecification.Builder()
    .withId(TABLE_ID)
    .withFields(new FieldType(NAMESPACE_COL, FieldType.Type.STRING),
                new FieldType(APP_GENERATION_COL, FieldType.Type.LONG),
                new FieldType(APPNAME_COL, FieldType.Type.STRING),
                new FieldType(INSTANCEID_COL, FieldType.Type.INTEGER),
                new FieldType(STATE_COL, FieldType.Type.STRING),
                new FieldType(VALUE_COL, FieldType.Type.BYTES),
                new FieldType(UPDATED_COL, FieldType.Type.LONG))
    .withPrimaryKeys(NAMESPACE_COL, APPNAME_COL, APP_GENERATION_COL, INSTANCEID_COL, STATE_COL)
    .build();

  private final StructuredTable table;

  private DBReplicationStateStore(StructuredTable table) {
    this.table = table;
  }

  static DBReplicationStateStore get(StructuredTableContext context) {
    try {
      StructuredTable table = context.getTable(TABLE_ID);
      return new DBReplicationStateStore(table);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(String.format(
        "System table '%s' does not exist. Please check your system environment.", TABLE_ID.getName()), e);
    }
  }

  public byte[] getStateData(DeltaWorkerId id, String state)
    throws IOException {
    List<Field<?>> keys = getKey(id, state);
    Optional<StructuredRow> row = table.read(keys);
    if (!row.isPresent() || row.get().getBytes(VALUE_COL) == null) {
      return new byte[0];
    }
    return row.get().getBytes(VALUE_COL);
  }

  public void writeStateData(DeltaWorkerId id, String state, byte[] data) throws IOException {
    Collection<Field<?>> fields = getKey(id, state);
    fields.add(Fields.bytesField(VALUE_COL, data));
    fields.add(Fields.longField(UPDATED_COL, System.currentTimeMillis()));

    table.upsert(fields);
  }

  public Long getLatestGeneration(String namespace, String appName) throws IOException {
    List<Field<?>> prefix = new ArrayList<>(2);
    prefix.add(Fields.stringField(NAMESPACE_COL, namespace));
    prefix.add(Fields.stringField(APPNAME_COL, appName));
    Range range = Range.singleton(prefix);
    long maxGeneration = -1L;
    try (CloseableIterator<StructuredRow> rowIter = table.scan(range, Integer.MAX_VALUE)) {
      while (rowIter.hasNext()) {
        long gen = rowIter.next().getLong(APP_GENERATION_COL);
        maxGeneration = Math.max(gen, maxGeneration);
      }
    }
    return maxGeneration;
  }

  public Collection<Integer> getWorkerInstances(String namespace, String appName, Long generation) throws IOException {
    List<Field<?>> prefix = new ArrayList<>(3);
    prefix.add(Fields.stringField(NAMESPACE_COL, namespace));
    prefix.add(Fields.stringField(APPNAME_COL, appName));
    prefix.add(Fields.longField(APP_GENERATION_COL, generation));
    Range range = Range.singleton(prefix);
    HashSet<Integer> instancesSet = new HashSet<>();
    try (CloseableIterator<StructuredRow> rowIter = table.scan(range, Integer.MAX_VALUE)) {
      while (rowIter.hasNext()) {
        instancesSet.add(rowIter.next().getInteger(INSTANCEID_COL));
      }
    }
    List<Integer> workerInstances = new ArrayList<>(instancesSet);
    return workerInstances;
  }

  private List<Field<?>> getKey(DeltaWorkerId id, String state) {
    List<Field<?>> keyFields = new ArrayList<>(5);
    keyFields.add(Fields.stringField(NAMESPACE_COL, id.getPipelineId().getNamespace()));
    keyFields.add(Fields.stringField(APPNAME_COL, id.getPipelineId().getApp()));
    keyFields.add(Fields.longField(APP_GENERATION_COL, id.getPipelineId().getGeneration()));
    keyFields.add(Fields.intField(INSTANCEID_COL, id.getInstanceId()));
    keyFields.add(Fields.stringField(STATE_COL, state));
    return keyFields;
  }
}
