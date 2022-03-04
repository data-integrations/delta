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

import io.cdap.delta.api.assessment.Problem;

import java.util.List;
import java.util.Objects;

/**
 * Full table assessment using assessments from both the source and target, and containing any problems that may
 * occur if the table were to be replicated as-is.
 */
public class TableAssessmentResponse {
  private final List<FullColumnAssessment> columns;
  private final List<Problem> features;
  private final List<Problem> transformationIssues;

  public TableAssessmentResponse(List<FullColumnAssessment> columns, List<Problem> features,
                                 List<Problem> transformationIssues) {
    this.columns = columns;
    this.features = features;
    this.transformationIssues = transformationIssues;
  }

  public List<FullColumnAssessment> getColumns() {
    return columns;
  }

  public List<Problem> getFeatures() {
    return features;
  }

  public List<Problem> getTransformationIssues() {
    return transformationIssues;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableAssessmentResponse that = (TableAssessmentResponse) o;
    return Objects.equals(columns, that.columns) &&
      Objects.equals(features, that.features) &&
      Objects.equals(transformationIssues, that.transformationIssues);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columns, features, transformationIssues);
  }
}
