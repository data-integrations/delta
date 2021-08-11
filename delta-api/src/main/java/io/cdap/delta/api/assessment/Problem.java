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

/**
 * An assessment about some type of entity.
 */
public class Problem {
  private final String name;
  private final String description;
  private final String suggestion;
  private final String impact;
  private final boolean isBlocker;

  public Problem(String name, String description, String suggestion, String impact, boolean isBlocker) {
    this.name = name;
    this.description = description;
    this.suggestion = suggestion;
    this.impact = impact;
    this.isBlocker = isBlocker;
  }

  public Problem(String name, String description, String suggestion, String impact) {
    this(name, description, suggestion, impact, true);
  }

  /**
   * @return name of the entity that is being assessed
   */
  public String getName() {
    return name;
  }

  /**
   * @return description of what might be wrong
   */
  public String getDescription() {
    return description;
  }

  /**
   * @return suggestion for what to do next to fix the potential problem
   */
  public String getSuggestion() {
    return suggestion;
  }

  /**
   * @return impact of the potential problem
   */
  public String getImpact() {
    return impact;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Problem that = (Problem) o;
    return Objects.equals(name, that.name) &&
      Objects.equals(description, that.description) &&
      Objects.equals(suggestion, that.suggestion) &&
      Objects.equals(impact, that.impact);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, description, suggestion, impact);
  }
}
