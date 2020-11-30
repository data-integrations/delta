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

package io.cdap.delta.api;

import java.util.Objects;

/**
 * Represents properties of source in replicator pipeline that are exposed to the targets.
 */
public class SourceProperties {

  /**
   * Enum describing ordering of events.
   */
  public enum Ordering {
    ORDERED,
    UN_ORDERED
  }

  private final Ordering ordering;

  private SourceProperties(Ordering ordering) {
    this.ordering = ordering;
  }

  public Ordering getOrdering() {
    return ordering;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SourceProperties that = (SourceProperties) o;
    return ordering == that.ordering;
  }

  @Override
  public int hashCode() {
    return Objects.hash(ordering);
  }

  /**
   * Builder for a source properties.
   */
  public static class Builder {
    private Ordering ordering;

    public Builder() {
      ordering = Ordering.ORDERED;
    }

    /**
     * Sets the ordering of the events generated by source.
     *
     * @return this builder
     */
    public SourceProperties.Builder setOrdering(Ordering ordering) {
      this.ordering = ordering;
      return this;
    }

    /**
     * @return an instance of {@code SourceProperties}
     */
    public SourceProperties build() throws IllegalArgumentException {
      return new SourceProperties(ordering);
    }
  }
}
