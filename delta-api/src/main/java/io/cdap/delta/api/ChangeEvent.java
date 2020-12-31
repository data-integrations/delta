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
import javax.annotation.Nullable;

/**
 * A change event.
 */
public abstract class ChangeEvent {
  private final Offset offset;
  private final boolean isSnapshot;
  private final ChangeType changeType;
  private final Long sourceTimestampMillis;

  protected ChangeEvent(Offset offset, boolean isSnapshot, ChangeType changeType,
                        @Nullable Long sourceTimestampMillis) {
    this.offset = offset;
    this.isSnapshot = isSnapshot;
    this.changeType = changeType;
    this.sourceTimestampMillis = sourceTimestampMillis;
  }

  public Offset getOffset() {
    return offset;
  }

  public boolean isSnapshot() {
    return isSnapshot;
  }

  public ChangeType getChangeType() {
    return changeType;
  }

  /**
   *
   * {@link SourceProperties.Ordering#UN_ORDERED unordered} should return a non null value.
   * and this value should be able to identify the order of the event.
   * @return the time that indicate when this event was committed on the source
   */
  @Nullable
  public Long getSourceTimestampMillis() {
    return sourceTimestampMillis;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ChangeEvent that = (ChangeEvent) o;
    return isSnapshot == that.isSnapshot &&
      Objects.equals(offset, that.offset) &&
      Objects.equals(sourceTimestampMillis, that.sourceTimestampMillis) &&
      changeType == that.changeType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(offset, isSnapshot, changeType, sourceTimestampMillis);
  }

  /**
   * Builds a ChangeEvent
   *
   * @param <T> type of builder
   */
  public static class Builder<T extends Builder> {
    protected Offset offset;
    protected boolean isSnapshot;
    protected Long sourceTimestampMillis;

    public T setOffset(Offset offset) {
      this.offset = offset;
      return (T) this;
    }

    public T setSnapshot(boolean isSnapshot) {
      this.isSnapshot = isSnapshot;
      return (T) this;
    }

    /**
     * Set the source timestamp for this change event. This value is used in determining the ordering
     * of events when source generates the un-ordered events. Not setting this for such sources could result
     * into the incorrect data on target.
     */
    public T setSourceTimestamp(long sourceTimestampMillis) {
      this.sourceTimestampMillis = sourceTimestampMillis;
      return (T) this;
    }
  }
}
