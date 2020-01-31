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

package io.cdap.delta.store;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaTarget;
import io.cdap.delta.api.assessment.StandardizedTableDetail;
import io.cdap.delta.api.assessment.TableAssessment;
import io.cdap.delta.api.assessment.TableAssessor;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableRegistry;
import io.cdap.delta.test.mock.MockTableAssessor;
import io.cdap.delta.test.mock.MockTableRegistry;

import java.sql.SQLType;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Mock configurer that returns mock plugins based on serialized objects passed in through plugin properties.
 */
public class PropertyBasedMockConfigurer implements Configurer {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeHierarchyAdapter(SQLType.class, new SQLTypeSerde())
    .create();
  private static final String SOURCE_TABLE_LIST = "source.table.list";
  private static final String SOURCE_TABLE_DETAIL = "source.table.detail";
  private static final String SOURCE_SCHEMA = "source.schema";
  private static final String SOURCE_ASSESSMENT = "source.assessment";
  private static final String TARGET_ASSESSMENT = "target.assessment";

  @Nullable
  @Override
  @SuppressWarnings("unchecked")
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                         PluginSelector selector) {
    Map<String, String> props = properties.getProperties();
    if (DeltaSource.PLUGIN_TYPE.equals(pluginType)) {
      TableList tableList = GSON.fromJson(props.get(SOURCE_TABLE_LIST), TableList.class);
      TableDetail tableDetail = GSON.fromJson(props.get(SOURCE_TABLE_DETAIL), TableDetail.class);
      Schema schema = GSON.fromJson(props.get(SOURCE_SCHEMA), Schema.class);
      TableRegistry tableRegistry = new MockTableRegistry(tableList, tableDetail, schema);

      TableAssessment assessment = GSON.fromJson(props.get(SOURCE_ASSESSMENT), TableAssessment.class);
      TableAssessor<TableDetail> tableAssessor = new MockTableAssessor<>(assessment);
      return (T) new MockSource(tableRegistry, tableAssessor);
    } else if (DeltaTarget.PLUGIN_TYPE.equals(pluginType)) {
      TableAssessment assessment = GSON.fromJson(props.get(TARGET_ASSESSMENT), TableAssessment.class);
      TableAssessor<StandardizedTableDetail> tableAssessor = new MockTableAssessor<>(assessment);
      return (T) new MockTarget(tableAssessor);
    }
    return null;
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId,
                                     PluginProperties properties, PluginSelector selector) {
    return null;
  }

  /**
   * return a map of properties that can be used to create a mock source or target that return the given objects.
   */
  public static Map<String, String> createMap(TableList sourceList, TableDetail sourceDetail, Schema sourceSchema,
                                              TableAssessment sourceAssessment, TableAssessment targetAssessment) {
    Map<String, String> map = new HashMap<>();
    map.put(SOURCE_TABLE_LIST, GSON.toJson(sourceList));
    map.put(SOURCE_TABLE_DETAIL, GSON.toJson(sourceDetail));
    map.put(SOURCE_SCHEMA, GSON.toJson(sourceSchema));
    map.put(SOURCE_ASSESSMENT, GSON.toJson(sourceAssessment));
    map.put(TARGET_ASSESSMENT, GSON.toJson(targetAssessment));
    return map;
  }
}
