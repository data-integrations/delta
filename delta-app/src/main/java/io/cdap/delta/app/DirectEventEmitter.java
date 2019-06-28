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

import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.EventConsumer;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.Sequenced;

import java.io.IOException;

/**
 * In memory event emitter that simply passes the event directly to the delta target.
 */
public class DirectEventEmitter implements EventEmitter {
  private final EventConsumer consumer;
  private final DeltaContext context;
  private long sequenceNumber;

  public DirectEventEmitter(EventConsumer consumer, DeltaContext context, long sequenceNumber) {
    this.consumer = consumer;
    this.context = context;
    this.sequenceNumber = sequenceNumber;
  }

  @Override
  public void emit(DDLEvent event) {
    consumer.applyDDL(new Sequenced<>(event, sequenceNumber));
    try {
      context.commitOffset(event.getOffset());
    } catch (IOException e) {
      // TODO: handle the error, retry
      throw new RuntimeException(e);
    } finally {
      sequenceNumber++;
    }
  }

  @Override
  public void emit(DMLEvent event) {
    consumer.applyDML(new Sequenced<>(event, sequenceNumber));
    sequenceNumber++;
  }
}
