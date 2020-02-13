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

package io.cdap.delta.api.assessment;

import java.sql.SQLType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Details about a table column.
 */
public class ColumnDetail {
  private final String name;
  private final SQLType type;
  private final boolean nullable;
  private final Map<String, String> properties;

  public ColumnDetail(String name, SQLType type, boolean nullable) {
    this(name, type, nullable, new HashMap<>());
  }

  public ColumnDetail(String name, SQLType type, boolean nullable, Map<String, String> properties) {
    this.name = name;
    this.type = type;
    this.nullable = nullable;
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
  }

  public String getName() {
    return name;
  }

  public SQLType getType() {
    return type;
  }

  public boolean isNullable() {
    return nullable;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnDetail that = (ColumnDetail) o;
    return nullable == that.nullable &&
      Objects.equals(name, that.name) &&
      Objects.equals(type, that.type) &&
      Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, nullable, properties);
  }
}
