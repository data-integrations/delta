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
 * Notifies the application framework that there is a failure.
 */
public interface FailureNotifier {

  /**
   * Call this method if a failure was encountered.
   * In the event of a failure, the application framework will stop the EventReader and EventConsumer,
   * create new ones, then restart from the last committed offset.
   * This will be retried up to the retry timeout configured for the program.
   *
   * If the cause is a {@link DeltaFailureException}, the program will be failed immediately instead of retried.
   *
   * @param cause cause of the failure
   */
  void notifyFailed(Throwable cause);

}
