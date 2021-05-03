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

/**
 * Reads change events.
 */
public interface EventReader {

  /**
   * Start reading events from the given offset.
   */
  void start(Offset offset);

  /**
   * Stop reading events.
   *
   * @throws InterruptedException if the reader was interrupted while stopping
   */
  default void stop() throws InterruptedException {
    stop(StopReason.UNKNOWN);
  }

  /**
   * Stop reading events.
   * @param reason the reason why stop is requested.
   * @throws InterruptedException if the reader was interrupted while stopping
   */
  default void stop(StopReason reason) throws InterruptedException {
    // no-op
  }

  public enum StopReason {
    UNKNOWN,
    RESTART_DUE_TO_ERROR,
    REQUESTED_BY_CUSTOMER
  }
}
