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

package io.cdap.delta.store;

import io.cdap.delta.proto.DeltaConfig;

import java.util.Objects;

/**
 * A pipeline draft.
 */
public class Draft {
  private final DeltaConfig config;
  private final long createdTimeMillis;
  private final long updatedTimeMillis;

  public Draft(DeltaConfig config, long createdTimeMillis, long updatedTimeMillis) {
    this.config = config;
    this.createdTimeMillis = createdTimeMillis;
    this.updatedTimeMillis = updatedTimeMillis;
  }

  public DeltaConfig getConfig() {
    return config;
  }

  public long getCreatedTimeMillis() {
    return createdTimeMillis;
  }

  public long getUpdatedTimeMillis() {
    return updatedTimeMillis;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Draft draft = (Draft) o;
    return createdTimeMillis == draft.createdTimeMillis &&
      updatedTimeMillis == draft.updatedTimeMillis &&
      Objects.equals(config, draft.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(config, createdTimeMillis, updatedTimeMillis);
  }
}
