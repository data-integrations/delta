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

package io.cdap.delta.proto;

import io.cdap.delta.api.assessment.ColumnSuggestion;
import io.cdap.delta.api.assessment.ColumnSupport;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Column assessment containing information from both the source and target. Types are system specific type names.
 * For example, when reading from a database, the source type should be a type familiar to users of those databases.
 * Similarly, the target type should be a type familiar to users of that target system.
 * Column names in the source and target may also differ, either because of a user specified transformation, or because
 * the source name had to be normalized because the target system has a different supported character set.
 */
public class FullColumnAssessment {
  private final ColumnSupport support;
  private final String sourceName;
  private final String sourceType;
  private final String targetName;
  private final String targetType;
  private final ColumnSuggestion suggestion;

  public FullColumnAssessment(ColumnSupport support, @Nullable String sourceName, @Nullable String sourceType,
                              @Nullable String targetName, @Nullable String targetType,
                              @Nullable ColumnSuggestion suggestion) {
    this.support = support;
    this.sourceName = sourceName;
    this.sourceType = sourceType;
    this.targetName = targetName;
    this.targetType = targetType;
    this.suggestion = suggestion;
  }

  public ColumnSupport getSupport() {
    return support;
  }

  @Nullable
  public String getSourceName() {
    return sourceName;
  }

  @Nullable
  public String getSourceType() {
    return sourceType;
  }

  @Nullable
  public String getTargetName() {
    return targetName;
  }

  @Nullable
  public String getTargetType() {
    return targetType;
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
    FullColumnAssessment that = (FullColumnAssessment) o;
    return support == that.support &&
      Objects.equals(sourceName, that.sourceName) &&
      Objects.equals(sourceType, that.sourceType) &&
      Objects.equals(targetName, that.targetName) &&
      Objects.equals(targetType, that.targetType) &&
      Objects.equals(suggestion, that.suggestion);
  }

  @Override
  public int hashCode() {
    return Objects.hash(support, sourceName, sourceType, targetName, targetType, suggestion);
  }
}
