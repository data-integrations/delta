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

package io.cdap.transformation;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.transformation.api.RowSchema;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Default implementation of {@RowSchema}
 */
public class DefaultRowSchema implements RowSchema {

  private Schema schema;
  private Map<String, Schema.Field> fieldsMap;
  private Map<String, String> originalToNewNames;
  private Map<String, String> newToOriginalNames;
  private boolean changed;

  public DefaultRowSchema(Schema schema) {
    if (schema == null) {
      throw new NullPointerException("Schema is null.");
    }
    if (schema.getType() != Schema.Type.RECORD) {
      throw new IllegalArgumentException("Schema type is not RECORD.");
    }
    this.schema = schema;
    this.fieldsMap = schema.getFields().stream().collect(Collectors.toMap(Schema.Field::getName, Function.identity(),
                                                                          (f1, f2) -> f2, LinkedHashMap::new));
    this.originalToNewNames = new HashMap<>();
    this.newToOriginalNames = new HashMap<>();
  }

  @Override
  public Schema.Field getField(String columnName) {
    if (!fieldsMap.containsKey(columnName)) {
      throw new IllegalArgumentException("Column name %s doesn't not exist.");
    }
    return fieldsMap.get(columnName);
  }


  @Override
  public void setField(String columnName, Schema.Field field) {
    if (columnName == null) {
      throw new NullPointerException("Column name is null.");
    }
    if (field == null) {
      throw new NullPointerException("Field is null.");
    }
    changed = true;
    String newName = field.getName();
    // handle the rename case
    if (!columnName.equals(newName)) {
      Set<String> names = fieldsMap.keySet();
      if (!names.contains(columnName)) {
        // it's adding a new column
        throw new IllegalArgumentException(String.format("New column %s has a different field name: %s.", columnName,
                                                         newName));
      }
      if (names.contains(newName)) {
        throw new IllegalArgumentException(String.format("Column name %s already exists.", newName));
      }
      recordRename(columnName, newName);
      fieldsMap.remove(columnName);
    }
    fieldsMap.put(newName, field);
  }

  private void recordRename(String originalName, String newName) {
    // check whether originalName actually was a new name
    String evenOlderName = newToOriginalNames.get(originalName);
    if (evenOlderName == null) {
      originalToNewNames.put(originalName, newName);
      newToOriginalNames.put(newName, originalName);
      return;
    }
    newToOriginalNames.remove(originalName);
    if (evenOlderName.equals(newName)) {
      originalToNewNames.remove(evenOlderName);
      return;
    }
    originalToNewNames.put(evenOlderName, newName);
    newToOriginalNames.put(newName, evenOlderName);
    return;

  }

  @Override
  public Schema toSchema() {
    if (changed) {
      schema = Schema.recordOf(schema.getRecordName(), fieldsMap.values());
    }
    return schema;
  }

  public ColumnRenameInfo getRenameInfo() {
    return new DefaultRenameInfo(originalToNewNames);
  }
}
