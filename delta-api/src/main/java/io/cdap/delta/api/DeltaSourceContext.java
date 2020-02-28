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

import java.io.IOException;

/**
 * Context for a CDC Source.
 */
public interface DeltaSourceContext extends DeltaRuntimeContext, FailureNotifier {

  /**
   * Record that there are currently errors reading change events.
   *
   * @param error information about the error
   * @throws IOException if there was an error persisting the change in state. This can only be thrown if there was
   *   a change in state.
   */
  void setError(ReplicationError error) throws IOException;

  /**
   * Record that there are no errors reading change events.
   *
   * @throws IOException if there was an error persisting the change in state. This can only be thrown if there was
   *   a change in state.
   */
  void setOK() throws IOException;
}
