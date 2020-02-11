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

package io.cdap.delta.app;

import io.cdap.delta.api.Offset;

/**
 * An offset and sequence number.
 */
public class OffsetAndSequence {
  private final Offset offset;
  private final long sequenceNumber;

  public OffsetAndSequence(Offset offset, long sequenceNumber) {
    this.offset = offset;
    this.sequenceNumber = sequenceNumber;
  }

  public Offset getOffset() {
    return offset;
  }

  public long getSequenceNumber() {
    return sequenceNumber;
  }
}
