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

import com.google.gson.Gson;
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
import io.cdap.delta.proto.DeltaConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Stores pipeline drafts.
 */
public class DraftStore {
  private static final Gson GSON = new Gson();
  private static final StructuredTableId TABLE_ID = new StructuredTableId("delta_drafts");
  private static final String NAMESPACE_COL = "namespace";
  private static final String GENERATION_COL = "generation";
  private static final String NAME_COL = "name";
  private static final String CREATED_COL = "created";
  private static final String UPDATED_COL = "updated";
  private static final String CONFIG_COL = "config";
  public static final StructuredTableSpecification TABLE_SPEC = new StructuredTableSpecification.Builder()
    .withId(TABLE_ID)
    .withFields(new FieldType(NAMESPACE_COL, FieldType.Type.STRING),
                new FieldType(GENERATION_COL, FieldType.Type.LONG),
                new FieldType(NAME_COL, FieldType.Type.STRING),
                new FieldType(CREATED_COL, FieldType.Type.LONG),
                new FieldType(UPDATED_COL, FieldType.Type.LONG),
                new FieldType(CONFIG_COL, FieldType.Type.BYTES))
    .withPrimaryKeys(NAMESPACE_COL, GENERATION_COL, NAME_COL)
    .build();

  private final StructuredTable table;

  private DraftStore(StructuredTable table) {
    this.table = table;
  }

  static DraftStore get(StructuredTableContext context) {
    try {
      StructuredTable table = context.getTable(TABLE_ID);
      return new DraftStore(table);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(String.format(
        "System table '%s' does not exist. Please check your system environment.", TABLE_ID.getName()), e);
    }
  }

  public List<Draft> listDrafts(Namespace namespace) throws IOException {
    List<Field<?>> prefix = new ArrayList<>(2);
    prefix.add(Fields.stringField(NAMESPACE_COL, namespace.getName()));
    prefix.add(Fields.longField(GENERATION_COL, namespace.getGeneration()));
    Range range = Range.singleton(prefix);
    List<Draft> results = new ArrayList<>();
    try (CloseableIterator<StructuredRow> rowIter = table.scan(range, Integer.MAX_VALUE)) {
      while (rowIter.hasNext()) {
        results.add(fromRow(rowIter.next()));
      }
    }
    return results;
  }

  public Optional<Draft> getDraft(DraftId id) throws IOException {
    Optional<StructuredRow> row = table.read(getKey(id));
    return row.map(this::fromRow);
  }

  public void deleteDraft(DraftId id) throws IOException {
    table.delete(getKey(id));
  }

  public void writeDraft(DraftId id, DeltaConfig config) throws IOException {
    Optional<Draft> existing = getDraft(id);
    long now = System.currentTimeMillis();
    long createTime = existing.map(Draft::getCreatedTimeMillis).orElse(now);
    long updatedTime = existing.map(Draft::getCreatedTimeMillis).orElse(now);
    table.upsert(getRow(id, new Draft(config, createTime, updatedTime)));
  }

  private void addKeyFields(DraftId id, List<Field<?>> fields) {
    fields.add(Fields.stringField(NAMESPACE_COL, id.getNamespace().getName()));
    fields.add(Fields.longField(GENERATION_COL, id.getNamespace().getGeneration()));
    fields.add(Fields.stringField(NAME_COL, id.getName()));
  }

  private List<Field<?>> getKey(DraftId id) {
    List<Field<?>> keyFields = new ArrayList<>(3);
    addKeyFields(id, keyFields);
    return keyFields;
  }

  private List<Field<?>> getRow(DraftId id, Draft draft) {
    List<Field<?>> fields = new ArrayList<>(6);
    addKeyFields(id, fields);
    fields.add(Fields.longField(CREATED_COL, draft.getCreatedTimeMillis()));
    fields.add(Fields.longField(UPDATED_COL, draft.getUpdatedTimeMillis()));
    fields.add(Fields.bytesField(CONFIG_COL, GSON.toJson(draft.getConfig()).getBytes(StandardCharsets.UTF_8)));
    return fields;
  }

  @SuppressWarnings("ConstantConditions")
  private Draft fromRow(StructuredRow row) {
    long createTime = row.getLong(CREATED_COL);
    long updateTime = row.getLong(UPDATED_COL);
    String configStr = new String(row.getBytes(CONFIG_COL), StandardCharsets.UTF_8);
    DeltaConfig config = GSON.fromJson(configStr, DeltaConfig.class);
    return new Draft(config, createTime, updateTime);
  }
}
