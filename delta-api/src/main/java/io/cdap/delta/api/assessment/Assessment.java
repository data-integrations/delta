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
 * A general assessment, containing potential problems.
 */
public class Assessment {
  private final List<Problem> features;
  private final List<Problem> connectivity;
  private final List<Problem> transformationIssues;

  public Assessment(List<Problem> features, List<Problem> connectivity) {
    this(features, connectivity, Collections.emptyList());
  }

  public Assessment(List<Problem> features, List<Problem> connectivity, List<Problem> transformationIssues) {
    this.features = Collections.unmodifiableList(new ArrayList<>(features));
    this.connectivity = Collections.unmodifiableList(new ArrayList<>(connectivity));
    this.transformationIssues = Collections.unmodifiableList(new ArrayList<>(transformationIssues));
  }

  /**
   * @return potential problems related to features
   */
  public List<Problem> getFeatures() {
    return features;
  }

  /**
   * @return potential problems related to connectivity
   */
  public List<Problem> getConnectivity() {
    return connectivity;
  }

  /**
   * @return potential problems related to transformation
   */
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

    Assessment that = (Assessment) o;
    return Objects.equals(features, that.features) &&
      Objects.equals(connectivity, that.connectivity) &&
      Objects.equals(transformationIssues, that.transformationIssues);
  }

  @Override
  public int hashCode() {
    return Objects.hash(features, connectivity, transformationIssues);
  }
}
