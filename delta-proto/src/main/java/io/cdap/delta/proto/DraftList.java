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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * List of drafts.
 */
public class DraftList {
  private final List<DeltaConfig> drafts;

  public DraftList(List<DeltaConfig> drafts) {
    this.drafts = Collections.unmodifiableList(new ArrayList<>(drafts));
  }

  public List<DeltaConfig> getDrafts() {
    return drafts;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DraftList draftList = (DraftList) o;
    return Objects.equals(drafts, draftList.drafts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(drafts);
  }
}
