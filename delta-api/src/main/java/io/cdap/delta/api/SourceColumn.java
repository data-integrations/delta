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

package io.cdap.delta.api;

import java.util.Objects;

/**
 * Information about a column to read from a source table.
 *
 * In the future, this may contain additional information, such as transformations to perform on the column.
 */
public class SourceColumn {
  private final String name;
  private final boolean suppressWarning;

  public SourceColumn(String name) {
    this(name, false);
  }

  public SourceColumn(String name, boolean suppressWarning) {
    this.name = name;
    this.suppressWarning = suppressWarning;
  }

  public String getName() {
    return name;
  }

  public boolean isSuppressWarning() {
    return suppressWarning;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SourceColumn that = (SourceColumn) o;
    return Objects.equals(name, that.name) && Objects.equals(suppressWarning, that.suppressWarning);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, suppressWarning);
  }
}
