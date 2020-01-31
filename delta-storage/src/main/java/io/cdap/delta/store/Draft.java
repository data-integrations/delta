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
import io.cdap.delta.proto.DraftRequest;

import java.util.Objects;

/**
 * A pipeline draft.
 */
public class Draft extends DraftRequest {
  private final String name;
  private final long createdTimeMillis;
  private final long updatedTimeMillis;

  public Draft(String name, String label, DeltaConfig config, long createdTimeMillis, long updatedTimeMillis) {
    super(label, config);
    this.name = name;
    this.createdTimeMillis = createdTimeMillis;
    this.updatedTimeMillis = updatedTimeMillis;
  }


  public long getCreatedTimeMillis() {
    return createdTimeMillis;
  }

  public long getUpdatedTimeMillis() {
    return updatedTimeMillis;
  }

  public String getName() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    Draft draft = (Draft) o;
    return createdTimeMillis == draft.createdTimeMillis &&
      updatedTimeMillis == draft.updatedTimeMillis &&
      Objects.equals(name, draft.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), name, createdTimeMillis, updatedTimeMillis);
  }
}
