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
  private final String sourceName;
  private final ColumnSupport support;
  private final ColumnSuggestion suggestion;

  private ColumnAssessment(ColumnSupport support, String name, String type,
                           @Nullable String sourceName, @Nullable ColumnSuggestion suggestion) {
    this.name = name;
    this.type = type;
    this.sourceName = sourceName;
    this.support = support;
    this.suggestion = suggestion;
  }

  /**
   * @return the name of the column as seen by the source. This only needs to be set for column assessments on the
   *   target, in case the column name was normalized or transformed in some way.
   */
  @Nullable
  public String getSourceName() {
    return sourceName;
  }

  /**
   * @return name of the column
   */
  public String getName() {
    return name;
  }

  /**
   * @return type of the column
   */
  public String getType() {
    return type;
  }

  /**
   * @return level of support for the column
   */
  public ColumnSupport getSupport() {
    return support;
  }

  /**
   * @return a suggestion to improve support level for the column
   */
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
    return Objects.equals(name, that.name) &&
      Objects.equals(type, that.type) &&
      Objects.equals(sourceName, that.sourceName) &&
      support == that.support &&
      Objects.equals(suggestion, that.suggestion);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, sourceName, support, suggestion);
  }

  /**
   * return a Builder to create column assessments
   */
  public static Builder builder(String name, String type) {
    return new Builder(name, type);
  }

  /**
   * Builds a ColumnAssessment
   */
  public static class Builder {
    private String name;
    private String type;
    private String sourceName;
    private ColumnSupport support;
    private ColumnSuggestion suggestion;

    private Builder(String name, String type) {
      this.name = name;
      this.type = type;
      this.support = ColumnSupport.YES;
    }

    public Builder setSupport(ColumnSupport support) {
      this.support = support;
      return this;
    }

    public Builder setSuggestion(ColumnSuggestion suggestion) {
      this.suggestion = suggestion;
      return this;
    }

    public Builder setSourceColumn(String sourceColumnName) {
      this.sourceName = sourceColumnName;
      return this;
    }

    public ColumnAssessment build() {
      return new ColumnAssessment(support, name, type, sourceName, suggestion);
    }
  }
}
