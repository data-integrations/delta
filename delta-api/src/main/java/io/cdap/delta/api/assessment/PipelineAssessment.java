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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * An assessment about a potential pipeline, indicating possible errors and fixes for those errors.
 */
public class PipelineAssessment {
  private final List<TableSummaryAssessment> tables;
  private final List<Problem> features;
  private final List<Problem> connectivity;

  public PipelineAssessment(List<TableSummaryAssessment> tables,
                            List<Problem> featureProblems,
                            List<Problem> connectivityProblems) {
    this.tables = Collections.unmodifiableList(new ArrayList<>(tables));
    this.features = Collections.unmodifiableList(new ArrayList<>(featureProblems));
    this.connectivity = Collections.unmodifiableList(new ArrayList<>(connectivityProblems));
  }

  /**
   * @return summary of issues related to tables
   */
  public List<TableSummaryAssessment> getTables() {
    return tables;
  }

  /**
   * @return potential problems related to features
   */
  public List<Problem> getFeatureProblems() {
    return features;
  }

  /**
   * @return potential problems related to connectivity
   */
  public List<Problem> getConnectivityProblem() {
    return connectivity;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PipelineAssessment that = (PipelineAssessment) o;
    return Objects.equals(tables, that.tables) &&
      Objects.equals(features, that.features) &&
      Objects.equals(connectivity, that.connectivity);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tables, features, connectivity);
  }
}
