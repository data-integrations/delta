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

import java.util.Objects;

/**
 * Configures how failures should be retried.
 */
public class RetryConfig {
  public static final RetryConfig DEFAULT = new RetryConfig(Integer.MAX_VALUE, 10);
  private final Integer maxDurationSeconds;
  private final Integer delaySeconds;

  public RetryConfig(int maxDurationSeconds, int delaySeconds) {
    this.maxDurationSeconds = maxDurationSeconds;
    this.delaySeconds = delaySeconds;
  }

  public int getMaxDurationSeconds() {
    return maxDurationSeconds == null ? Integer.MAX_VALUE : maxDurationSeconds;
  }

  public int getDelaySeconds() {
    return delaySeconds == null ? 10 : delaySeconds;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RetryConfig that = (RetryConfig) o;
    return maxDurationSeconds == that.maxDurationSeconds &&
      delaySeconds == that.delaySeconds;
  }

  @Override
  public int hashCode() {
    return Objects.hash(maxDurationSeconds, delaySeconds);
  }
}
