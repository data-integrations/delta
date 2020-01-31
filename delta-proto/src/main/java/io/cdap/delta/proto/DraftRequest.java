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

import java.util.Objects;

/**
 * Request to store a draft.
 */
public class DraftRequest {
  private final String label;
  private final DeltaConfig config;

  public DraftRequest(String label, DeltaConfig config) {
    this.label = label;
    this.config = config;
  }

  public String getLabel() {
    return label == null ? "" : label;
  }

  public DeltaConfig getConfig() {
    return config;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DraftRequest that = (DraftRequest) o;
    return Objects.equals(label, that.label) &&
      Objects.equals(config, that.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(label, config);
  }
}
