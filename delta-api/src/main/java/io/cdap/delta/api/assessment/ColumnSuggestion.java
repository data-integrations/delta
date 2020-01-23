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
 * Suggestion for how to fix a column problem.
 */
public class ColumnSuggestion {
  private final String message;
  private final List<String> transforms;

  public ColumnSuggestion(String message, List<String> transforms) {
    this.message = message;
    this.transforms = Collections.unmodifiableList(new ArrayList<>(transforms));
  }

  public String getMessage() {
    return message;
  }

  public List<String> getTransforms() {
    return transforms;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnSuggestion that = (ColumnSuggestion) o;
    return Objects.equals(message, that.message) &&
      Objects.equals(transforms, that.transforms);
  }

  @Override
  public int hashCode() {
    return Objects.hash(message, transforms);
  }
}
