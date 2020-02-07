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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.EventReaderDefinition;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.Sequenced;
import io.cdap.delta.api.SourceTable;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

/**
 * Tests for {@link DirectEventEmitter}.
 */
public class DirectEventEmitterTest {
  private static final Schema SCHEMA = Schema.recordOf("taybull", Schema.Field.of("id", Schema.of(Schema.Type.INT)));
  private static final DDLEvent DDL = DDLEvent.builder()
    .setOffset(new Offset(Collections.singletonMap("order", new byte[] { 0 })))
    .setOperation(DDLOperation.CREATE_TABLE)
    .setDatabase("deebee")
    .setTable("taybull")
    .setPrimaryKey(Collections.singletonList("id"))
    .setSchema(SCHEMA)
    .build();
  private static final DMLEvent DML = DMLEvent.builder()
    .setOffset(new Offset(Collections.singletonMap("order", new byte[] { 1 })))
    .setOperation(DMLOperation.INSERT)
    .setDatabase("deebee")
    .setTable("taybull")
    .setIngestTimestamp(1000L)
    .setRow(StructuredRecord.builder(SCHEMA).set("id", 0).build())
    .build();

  @Test
  public void testEmit() {
    TrackingEventConsumer trackingEventConsumer = new TrackingEventConsumer();
    EventReaderDefinition readerDefinition = new EventReaderDefinition(Collections.emptySet(), Collections.emptySet(),
                                                                       Collections.emptySet());
    DirectEventEmitter emitter = new DirectEventEmitter(trackingEventConsumer, null, 0L, readerDefinition);
    emitter.emit(DDL);
    emitter.emit(DML);

    Assert.assertEquals(Collections.singletonList(new Sequenced<>(DDL, 1L)), trackingEventConsumer.getDdlEvents());
    Assert.assertEquals(Collections.singletonList(new Sequenced<>(DML, 2L)), trackingEventConsumer.getDmlEvents());
  }

  @Test
  public void testFiltering() {
    TrackingEventConsumer trackingEventConsumer = new TrackingEventConsumer();
    Set<SourceTable> tables = Collections.singleton(
      new SourceTable("deebee", "taybull", null, Collections.emptySet(),
                      Collections.singleton(DMLOperation.INSERT),
                      Collections.singleton(DDLOperation.CREATE_TABLE)));
    EventReaderDefinition readerDefinition = new EventReaderDefinition(tables, Collections.emptySet(),
                                                                       Collections.emptySet());
    DirectEventEmitter emitter = new DirectEventEmitter(trackingEventConsumer, null, 0L, readerDefinition);
    emitter.emit(DDL);
    emitter.emit(DML);

    Assert.assertTrue(trackingEventConsumer.getDdlEvents().isEmpty());
    Assert.assertTrue(trackingEventConsumer.getDmlEvents().isEmpty());
  }

}
