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
 * A Change event with a sequence number.
 *
 * @param <T> type of event
 */
public class Sequenced<T> {
  private final T event;
  private final long sequenceNumber;

  public Sequenced(T event, long sequenceNumber) {
    this.event = event;
    this.sequenceNumber = sequenceNumber;
  }

  public T getEvent() {
    return event;
  }

  public long getSequenceNumber() {
    return sequenceNumber;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Sequenced<?> sequenced = (Sequenced<?>) o;
    return sequenceNumber == sequenced.sequenceNumber &&
      Objects.equals(event, sequenced.event);
  }

  @Override
  public int hashCode() {
    return Objects.hash(event, sequenceNumber);
  }
}
