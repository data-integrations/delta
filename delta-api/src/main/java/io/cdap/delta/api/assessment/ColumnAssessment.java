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

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Assessment of a column.
 */
public class ColumnAssessment {
  private final String name;
  private final String type;
  private final Support support;
  private final ColumnSuggestion suggestion;

  public ColumnAssessment(String name, String type) {
    this.name = name;
    this.type = type;
    this.support = Support.YES;
    this.suggestion = null;
  }

  public ColumnAssessment(String name, String type, Support support, ColumnSuggestion suggestion) {
    this.name = name;
    this.type = type;
    this.support = support;
    this.suggestion = suggestion;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public Support getSupport() {
    return support;
  }

  @Nullable
  public ColumnSuggestion getSuggestion() {
    return suggestion;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnAssessment that = (ColumnAssessment) o;
    return support == that.support &&
      Objects.equals(name, that.name) &&
      Objects.equals(type, that.type) &&
      Objects.equals(suggestion, that.suggestion);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, support, suggestion);
  }
}
