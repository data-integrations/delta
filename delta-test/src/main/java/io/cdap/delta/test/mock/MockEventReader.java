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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.delta.api.ChangeEvent;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.Offset;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * A mock event reader that emits a pre-specified list of events.
 */
public class MockEventReader implements EventReader {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(ChangeEvent.class, new ChangeEventDeserializer())
    .create();
  private final List<? extends ChangeEvent> events;
  private final EventEmitter emitter;
  private final int maxEvents;
  private int numEvents;

  public MockEventReader(List<? extends ChangeEvent> events, EventEmitter emitter, int maxEvents) {
    this.events = events;
    this.emitter = emitter;
    this.maxEvents = maxEvents;
    this.numEvents = 0;
  }

  @Override
  public void start(Offset offset) {
    Iterator<? extends ChangeEvent> eventIter = events.iterator();
    if (!offset.get().isEmpty()) {
      while (eventIter.hasNext()) {
        if (offsetsEqual(offset, eventIter.next().getOffset())) {
          break;
        }
      }
    }
    while (eventIter.hasNext() && numEvents < maxEvents) {
      numEvents++;
      ChangeEvent event = eventIter.next();
      if (event instanceof DDLEvent) {
        emitter.emit((DDLEvent) event);
      } else if (event instanceof DMLEvent) {
        emitter.emit((DMLEvent) event);
      }
    }
  }

  @Override
  public void stop() {
    // no-op
  }

  private boolean offsetsEqual(Offset o1, Offset o2) {
    if (!o1.get().keySet().equals(o2.get().keySet())) {
      return false;
    }
    for (String key : o1.get().keySet()) {
      if (!Arrays.equals(o1.get().get(key), o2.get().get(key))) {
        return false;
      }
    }
    return true;
  }
}
