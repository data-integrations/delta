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

package io.cdap.delta.test.mock;

import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.ReplicationError;

import java.io.IOException;

/**
 * An event reader that immediately notifies the framework that there is an error.
 */
public class FailureEventReader implements EventReader {
  private final DeltaSourceContext context;

  FailureEventReader(DeltaSourceContext context) {
    this.context = context;
  }

  @Override
  public void start(Offset offset) {
    RuntimeException exception = new RuntimeException("Source was configured to fail");
    try {
      context.setError(new ReplicationError(exception));
    } catch (IOException e) {
      // shouldn't happen in unit tests
    }
    context.notifyFailed(exception);
  }
}
