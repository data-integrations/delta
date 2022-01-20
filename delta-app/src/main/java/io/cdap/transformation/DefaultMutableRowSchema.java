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
import io.cdap.transformation.api.MutableRowSchema;
import io.cdap.transformation.api.NotFoundException;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Default implementation of {@RowSchema}
 */
public class DefaultMutableRowSchema implements MutableRowSchema {

  private Schema schema;
  private Map<String, Schema.Field> fieldsMap;
  private Map<String, String> originalToNewNames;
  private Map<String, String> newToOriginalNames;
  private boolean changed;

  public DefaultMutableRowSchema(Schema schema) {
    if (schema == null) {
      throw new IllegalArgumentException("Schema is null.");
    }
    if (schema.getType() != Schema.Type.RECORD) {
      throw new IllegalArgumentException(String.format("Expecting schema of type RECORD but got %s.",
                                                       schema.getType()));
    }
    this.schema = schema;
    this.fieldsMap = schema.getFields().stream().collect(Collectors.toMap(Schema.Field::getName, Function.identity(),
                                                                          (f1, f2) -> f2, LinkedHashMap::new));
    this.originalToNewNames = new HashMap<>();
    this.newToOriginalNames = new HashMap<>();
  }

  @Override
  public Schema.Field getField(String columnName) throws NotFoundException {
    if (!fieldsMap.containsKey(columnName)) {
      throw new NotFoundException("Column name %s doesn't not exist.");
    }
    return fieldsMap.get(columnName);
  }


  @Override
  public void setField(Schema.Field field) {
    if (field == null) {
      throw new IllegalArgumentException("Field is null.");
    }

    String name = field.getName();
    if (name == null) {
      throw new IllegalArgumentException("Field name is null.");
    }

    Schema.Field originalField = fieldsMap.get(name);
    if (originalField != null && originalField.getSchema().equals(field.getSchema())) {
      return;
    }
    changed = true;
    fieldsMap.put(name, field);
  }

  @Override
  public void renameField(String originalName, String newName) {
    Set<String> names = fieldsMap.keySet();
    if (originalName == null) {
      throw new IllegalArgumentException("Original field name is null.");
    }

    if (newName == null) {
      throw new IllegalArgumentException("New field name is null.");
    }

    if (!names.contains(originalName)) {
      throw new IllegalArgumentException(String.format("Original field name %s doesn't not exist.", originalName));
    }

    if (originalName.equals(newName)) {
      return;
    }

    if (names.contains(newName)) {
      throw new IllegalArgumentException(String.format("Field name %s already exists.", newName));
    }

    changed = true;
    recordRename(originalName, newName);
    Schema.Field originalField = fieldsMap.remove(originalName);
    fieldsMap.put(newName, Schema.Field.of(newName, originalField.getSchema()));
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
