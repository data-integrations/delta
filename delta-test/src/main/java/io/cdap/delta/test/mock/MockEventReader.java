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

import io.cdap.delta.api.ChangeEvent;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.Offset;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A mock event reader that emits a pre-specified list of events.
 */
public class MockEventReader implements EventReader {
  private final List<? extends ChangeEvent> events;
  private final EventEmitter emitter;
  private final int maxEvents;
  private final ExecutorService executorService;
  private final AtomicBoolean shouldStop;
  private int numEvents;

  public MockEventReader(List<? extends ChangeEvent> events, EventEmitter emitter, int maxEvents) {
    this.events = events;
    this.emitter = emitter;
    this.maxEvents = maxEvents;
    this.numEvents = 0;
    this.executorService = Executors.newSingleThreadExecutor();
    this.shouldStop = new AtomicBoolean(false);
  }

  @Override
  public void start(Offset offset) {
    Iterator<? extends ChangeEvent> eventIter = events.iterator();
    if (!offset.get().isEmpty()) {
      while (eventIter.hasNext()) {
        if (offset.equals(eventIter.next().getOffset())) {
          break;
        }
      }
    }
    executorService.submit(() -> {
      while (!shouldStop.get() && eventIter.hasNext() && numEvents < maxEvents) {
        numEvents++;
        ChangeEvent event = eventIter.next();
        try {
          if (event instanceof DDLEvent) {
            emitter.emit((DDLEvent) event);
          } else if (event instanceof DMLEvent) {
            emitter.emit((DMLEvent) event);
          }
        } catch (InterruptedException e) {
          shouldStop.set(true);
        }
      }
    });
  }

  @Override
  public void stop() {
    shouldStop.set(true);
  }
}
