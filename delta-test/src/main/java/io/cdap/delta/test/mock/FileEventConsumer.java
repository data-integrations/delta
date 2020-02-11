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
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.delta.api.ChangeEvent;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DeltaTargetContext;
import io.cdap.delta.api.EventConsumer;
import io.cdap.delta.api.Sequenced;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Buffers events in memory, then writes them out to a file when the consumer is closed.
 */
public class FileEventConsumer implements EventConsumer {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(StructuredRecord.class, new StructuredRecordSerde())
    .registerTypeAdapter(ChangeEvent.class, new ChangeEventDeserializer())
    .create();
  private final File file;
  private final List<ChangeEvent> events;
  private final DeltaTargetContext context;

  FileEventConsumer(File file, DeltaTargetContext context) {
    this.file = file;
    this.events = new ArrayList<>();
    this.context = context;
  }

  @Override
  public void start() {
    // no-op
  }

  @Override
  public void stop() {
    try (FileWriter writer = new FileWriter(file)) {
      writer.write(GSON.toJson(events));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void applyDDL(Sequenced<DDLEvent> event) throws IOException {
    events.add(event.getEvent());
    context.incrementCount(event.getEvent().getOperation());
    context.commitOffset(event.getEvent().getOffset(), event.getSequenceNumber());
    context.setTableReplicating(event.getEvent().getDatabase(), event.getEvent().getTable());
  }

  @Override
  public void applyDML(Sequenced<DMLEvent> event) throws IOException {
    events.add(event.getEvent());
    context.incrementCount(event.getEvent().getOperation());
    context.commitOffset(event.getEvent().getOffset(), event.getSequenceNumber());
  }

  /**
   * Read events that were consumed by the target. This should only be called after the pipeline has been stopped.
   *
   * @param filePath path to read events from
   * @return list of events contained in the file path
   * @throws IOException if there was an issue reading the file
   */
  public static List<? extends ChangeEvent> readEvents(File filePath) throws IOException {
    if (!filePath.exists()) {
      return Collections.emptyList();
    }
    try (Reader reader = new FileReader(filePath)) {
      return GSON.fromJson(reader, new TypeToken<List<? extends ChangeEvent>>() { }.getType());
    }
  }
}
