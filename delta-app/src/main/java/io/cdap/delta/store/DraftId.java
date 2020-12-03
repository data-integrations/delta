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

import java.util.Objects;

/**
 * Uniquely identifies a draft.
 */
public class DraftId {
  private final Namespace namespace;
  private final String name;

  public DraftId(Namespace namespace, String name) {
    this.namespace = namespace;
    this.name = name;
  }

  public Namespace getNamespace() {
    return namespace;
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
    DraftId draftId = (DraftId) o;
    return Objects.equals(namespace, draftId.namespace) &&
      Objects.equals(name, draftId.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, name);
  }

  @Override
  public String toString() {
    return namespace + "-" + name;
  }
}
