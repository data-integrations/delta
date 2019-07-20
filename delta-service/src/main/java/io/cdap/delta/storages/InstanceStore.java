/*
 *  Copyright © 2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.delta.storages;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
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
import io.cdap.delta.protos.Instance;
import org.apache.commons.codec.digest.DigestUtils;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Instance Store
 */
public class InstanceStore {
  private static final Gson GSON = new GsonBuilder()
          .registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();
  private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() { }.getType();
  private static final String NAMESPACE_COL = "namespace";
  private static final String ID_COL = "id";
  private static final String NAME_COL = "name";
  private static final String DESC_COL = "desription";
  private static final String PROPERTIES_COL = "properties";
  private static final String CREATED_COL = "created";
  private static final String UPDATED_COL = "updated";

  private static final StructuredTableId TABLE_ID = new StructuredTableId("delta_instances_1");
  public static final StructuredTableSpecification TABLE_SPEC = new StructuredTableSpecification.Builder()
          .withId(TABLE_ID)
          .withFields(new FieldType(NAMESPACE_COL, FieldType.Type.STRING),
                  new FieldType(ID_COL, FieldType.Type.STRING),
                  new FieldType(NAME_COL, FieldType.Type.STRING),
                  new FieldType(DESC_COL, FieldType.Type.STRING),
                  new FieldType(PROPERTIES_COL, FieldType.Type.STRING),
                  new FieldType(CREATED_COL, FieldType.Type.LONG),
                  new FieldType(UPDATED_COL, FieldType.Type.LONG))
          .withPrimaryKeys(NAMESPACE_COL, ID_COL)
          .build();

  private final StructuredTable table;

  private InstanceStore(StructuredTable table) {
    this.table = table;
  }

  public static InstanceStore get(StructuredTableContext context) {
    try {
      StructuredTable table = context.getTable(TABLE_ID);
      return new InstanceStore(table);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(String.format(
              "Table '%s' does not exist. Please check your environment.", TABLE_ID.getName()), e);
    }
  }

  public String create(String namespace, String instanceName, String description) throws IOException, RuntimeException {
    String id = DigestUtils.md5Hex(namespace.concat("!").concat(instanceName));

    Instance existing = read(namespace, id);
    if (existing != null) {
      throw new RuntimeException(String.format("Instance named %s already exists", instanceName));
    }

    long now = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());

    Instance instance = Instance.builder(id)
            .setName(instanceName)
            .setNamespace(namespace)
            .setCreated(now)
            .setUpdated(now)
            .setDescription(description)
            .build();

    table.upsert(toFields(instance));
    return instance.getId();
  }

  public List<Instance> list(String namespace) throws IOException {
    List<Field<?>> key = new ArrayList<>(1);
    key.add(Fields.stringField(NAMESPACE_COL, namespace));

    Range range = Range.singleton(key);

    try (CloseableIterator<StructuredRow> rowIter = table.scan(range, Integer.MAX_VALUE)) {
      List<Instance> result = new ArrayList<>();
      while (rowIter.hasNext()) {
        StructuredRow row = rowIter.next();
        Instance instance = fromRow(row);
        result.add(instance);
      }
      return result;
    }
  }

  public void update(String namespace, String id, Instance instance) throws IOException {
    Instance existing = read(namespace, id);

    Instance updated = Instance.builder(id, instance)
            .setNamespace(namespace)
            .setCreated(existing.getCreated())
            .setUpdated(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()))
            .build();
    table.upsert(toFields(updated));
  }

  public Instance get(String namespace, String id) throws RuntimeException, IOException {
    Instance existing = read(namespace, id);
    if (existing == null) {
      throw new RuntimeException(String.format("Instance %s does not exist", id));
    }
    return existing;
  }

  public Instance read(String namespace, String id) throws IOException {
    Optional<StructuredRow> row = table.read(getKey(namespace, id));
    return row.map(this::fromRow).orElse(null);
  }

  public void delete(String namespace, String id) throws IOException {
    table.delete(getKey(namespace, id));
  }

  private List<Field<?>> getKey(String namespace, String id) {
    List<Field<?>> keyFields = new ArrayList<>(2);
    keyFields.add(Fields.stringField(NAMESPACE_COL, namespace));
    keyFields.add(Fields.stringField(ID_COL, id));
    return keyFields;
  }

  private List<Field<?>> toFields(Instance instance) {
    List<Field<?>> fields = new ArrayList<>(7);
    fields.add(Fields.stringField(NAMESPACE_COL, instance.getNamespace()));
    fields.add(Fields.stringField(ID_COL, instance.getId()));
    fields.add(Fields.stringField(NAME_COL, instance.getName()));
    fields.add(Fields.stringField(DESC_COL, instance.getDescription()));
    fields.add(Fields.longField(CREATED_COL, instance.getCreated()));
    fields.add(Fields.longField(UPDATED_COL, instance.getUpdated()));
    fields.add(Fields.stringField(PROPERTIES_COL, GSON.toJson(instance.getProperties())));
    return fields;
  }

  private Instance fromRow(StructuredRow row) {
    return Instance.builder(row.getString(ID_COL))
            .setNamespace(row.getString(NAMESPACE_COL))
            .setName(row.getString(NAME_COL))
            .setDescription(row.getString(DESC_COL))
            .setCreated(row.getLong(CREATED_COL))
            .setUpdated(row.getLong(UPDATED_COL))
            .setProperties(GSON.fromJson(row.getString(PROPERTIES_COL), MAP_TYPE))
            .build();
  }
}
