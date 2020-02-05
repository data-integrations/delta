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
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.delta.api.ChangeEvent;
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.EventReaderDefinition;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.StandardizedTableDetail;
import io.cdap.delta.api.assessment.TableAssessor;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableRegistry;
import io.cdap.delta.proto.Artifact;
import io.cdap.delta.proto.Plugin;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Mock DeltaSource used in unit tests. Emits a pre-configured set of events.
 */
@io.cdap.cdap.api.annotation.Plugin(type = DeltaSource.PLUGIN_TYPE)
@Name(MockSource.NAME)
public class MockSource implements DeltaSource {
  public static final String NAME = "mock";
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(StructuredRecord.class, new StructuredRecordSerde())
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(ChangeEvent.class, new ChangeEventDeserializer())
    .create();
  private final Conf conf;

  public MockSource(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configure(Configurer configurer) {
    // no-op
  }

  @Override
  public EventReader createReader(EventReaderDefinition definition, DeltaSourceContext context, EventEmitter emitter) {
    return new MockEventReader(GSON.fromJson(conf.events, new TypeToken<List<? extends ChangeEvent>>() { }.getType()),
                               emitter, conf.maxEvents);
  }

  @Override
  public TableRegistry createTableRegistry(Configurer configurer) {
    // fake table registry returns null for everything.
    // this doesn't cause errors because TableRegistry is not used at runtime, only for creating pipelines
    return new TableRegistry() {
      @Override
      public TableList listTables() {
        return null;
      }

      @Override
      public TableDetail describeTable(String database, String table) {
        return null;
      }

      @Override
      public StandardizedTableDetail standardize(TableDetail tableDetail) {
        return null;
      }

      @Override
      public void close() {

      }
    };
  }

  @Override
  public TableAssessor<TableDetail> createTableAssessor(Configurer configurer) {
    return tableDescriptor -> null;
  }

  /**
   * Config for the plugin
   */
  private static class Conf extends PluginConfig {
    private String events;

    private int maxEvents;
  }

  /**
   * Get the plugin configuration for a mock source that should emit the given events in order.
   *
   * @param events events to emit in order
   * @return plugin configuration for the mock source
   */
  public static Plugin getPlugin(List<? extends ChangeEvent> events) {
    return getPlugin(events, events.size());
  }

  /**
   * Get the plugin configuration for a mock source that should emit the given events in order, up to the maximum
   * number of events given. When maxEvents is less than the size of the event list, the pipeline will have to be
   * restarted multiple times in order to emit all the events. This is used to test that the pipeline starts
   * from the correct offset.
   *
   * @param events events to emit in order
   * @param maxEvents maximum number of events to emit for a single run of the pipeline
   * @return plugin configuration for the mock source
   */
  public static Plugin getPlugin(List<? extends ChangeEvent> events, int maxEvents) {
    Map<String, String> properties = new HashMap<>();
    properties.put("events", GSON.toJson(events));
    properties.put("maxEvents", String.valueOf(maxEvents));
    return new Plugin(NAME, DeltaSource.PLUGIN_TYPE, properties, Artifact.EMPTY);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("events", new PluginPropertyField("events", "", "string", true, false));
    properties.put("maxEvents", new PluginPropertyField("maxEvents", "", "int", true, false));
    return new PluginClass(DeltaSource.PLUGIN_TYPE, NAME, "", MockSource.class.getName(), "conf", properties);
  }
}
