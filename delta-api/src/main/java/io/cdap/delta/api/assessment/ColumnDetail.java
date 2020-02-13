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
import java.util.Objects;

/**
 * Details about a table column.
 */
public class ColumnDetail {
  private final String name;
  private final SQLType type;
  private final boolean nullable;
  private final int length;
  private final int scale;

  public ColumnDetail(String name, SQLType type, boolean nullable, int length, int scale) {
    this.name = name;
    this.type = type;
    this.nullable = nullable;
    this.length = length;
    this.scale = scale;
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

  /**
   * Get the maximum length of this column's values. For numeric columns, this represents the precision.
   * @return the length of the column
   */
  public int getLength() {
    return length;
  }

  /**
   * Get the scale of the column.
   * @return the scale if it applies to this type; if not, return 0.
   */
  public int getScale() {
    return scale;
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
      length == that.length &&
      scale == that.scale &&
      Objects.equals(name, that.name) &&
      Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, nullable, length, scale);
  }
}
