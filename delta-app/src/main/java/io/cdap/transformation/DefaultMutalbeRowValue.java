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

import io.cdap.transformation.api.MutableRowValue;
import io.cdap.transformation.api.NotFoundException;

import java.util.Map;

/**
 * Default implementation of {@link MutableRowValue}
 */
public class DefaultMutalbeRowValue implements MutableRowValue {

  private final Map<String, Object> valuesMap;

  public DefaultMutalbeRowValue(Map<String, Object> valuesMap) {
    if (valuesMap == null) {
      throw new IllegalArgumentException("Values map is null");
    }
    this.valuesMap = valuesMap;
  }

  @Override
  public Object getColumnValue(String columnName) throws NotFoundException {
    if (!valuesMap.containsKey(columnName)) {
      throw new NotFoundException("Column name %s doesn't not exist.");
    }
    return valuesMap.get(columnName);
  }

  @Override
  public void setColumnValue(String columnName, Object value) {
    if (columnName == null) {
      throw new IllegalArgumentException("Column name is null.");
    }
    valuesMap.put(columnName, value);
  }

  @Override
  public void renameColumn(String originalName , String newName) {
    if (!valuesMap.containsKey(originalName)) {
      throw new IllegalArgumentException(String.format("Original column name %s doesn't not exist.", originalName));
    }
    if (valuesMap.containsKey(newName)) {
      throw new IllegalArgumentException(String.format("New column name %s already exists.", newName));
    }
    valuesMap.put(newName, valuesMap.remove(originalName));
  }

  public Map<String, Object> toValueMap() {
    return valuesMap;
  }
}
