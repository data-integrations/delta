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

import java.util.Arrays;
import java.util.Objects;

/**
 * A replication error, generally caused by a java exception. This is basically a serializable exception.
 */
public class ReplicationError {
  private final String message;
  private final StackTraceElement[] stackTrace;

  public ReplicationError(Throwable t) {
    this(t.getMessage(), t.getStackTrace());
  }

  public ReplicationError(String message, StackTraceElement[] stackTrace) {
    this.message = message;
    this.stackTrace = stackTrace;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReplicationError that = (ReplicationError) o;
    return Objects.equals(message, that.message) &&
      Arrays.equals(stackTrace, that.stackTrace);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(message);
    result = 31 * result + Arrays.hashCode(stackTrace);
    return result;
  }
}
