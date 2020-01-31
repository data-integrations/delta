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
 * A namespace name and generation.
 */
public class Namespace {
  private final String name;
  private final long generation;

  public Namespace(String name, long generation) {
    this.name = name;
    this.generation = generation;
  }

  public String getName() {
    return name;
  }

  public long getGeneration() {
    return generation;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Namespace namespace = (Namespace) o;
    return generation == namespace.generation &&
      Objects.equals(name, namespace.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, generation);
  }
}
