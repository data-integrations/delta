/*
 * Copyright © 2023 Cask Data, Inc.
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

import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;

public class MonitoredEventEmitter implements EventEmitter {
  private EventEmitter wrapped;
  private DeltaSourceContext deltaContext;

  public MonitoredEventEmitter(EventEmitter wrapped, DeltaSourceContext deltaContext) {
    this.wrapped = wrapped;
    this.deltaContext = deltaContext;
  }

  @Override
  public boolean emit(DDLEvent event) throws InterruptedException {
    boolean emitted = wrapped.emit(event);
    if(emitted){
      deltaContext.incrementPublishCount(event.getOperation());
    }
    return emitted;
  }

  @Override
  public boolean emit(DMLEvent event) throws InterruptedException {
    boolean emitted = wrapped.emit(event);
    if(emitted){
      deltaContext.incrementPublishCount(event.getOperation());
    }
    return emitted;
  }
}
