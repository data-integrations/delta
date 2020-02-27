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

import javax.annotation.Nullable;

/**
 * Request to get pipeline state.
 */
public class StateRequest {
  private final String name;
  private final String offsetBasePath;

  public StateRequest(String name, String offsetBasePath) {
    this.name = name;
    this.offsetBasePath = offsetBasePath;
  }

  public String getName() {
    return name;
  }

  @Nullable
  public String getOffsetBasePath() {
    return offsetBasePath;
  }

  public void validate() {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("Name must be specified.");
    }
  }
}
