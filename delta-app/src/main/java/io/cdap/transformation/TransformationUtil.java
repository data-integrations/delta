/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.transformation;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.proto.ColumnTransformation;
import io.cdap.delta.proto.DeltaConfig;
import io.cdap.delta.proto.TableTransformation;
import io.cdap.transformation.api.Transformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Utility for transformaiton plugin
 */
public class TransformationUtil {
  private TransformationUtil() {
  }

  public static String parseDirectiveName(String directive) {
    int index = directive.indexOf(" ");
    if (index < 0) {
      return directive;
    }
    return directive.substring(0, index);
  }

  /**
   * Apply a list of transformations on a row value
   *
   * @param valuesMap       the values map of the input row whose key is the column name and value is the column value.
   * @param transformations the list of transformations to apply
   * @return a map whose key is transformed column name and value is the transformed column value
   */
  public static Map<String, Object> transformValue(Map<String, Object> valuesMap, List<Transformation> transformations)
    throws Exception {
    DefaultMutalbeRowValue rowValue = new DefaultMutalbeRowValue(valuesMap);
    for (Transformation transformation : transformations) {
      transformation.transformValue(rowValue);
    }
    return rowValue.toValueMap();
  }

  /**
   * A helper method to reuse transformSchema with column level detail which is to be called during pipeline assessment
   * to track issues at a column level for better display of errors
   *
   * @param schema          the schema to apply transformation
   * @param transformations the map of Column to list of transformations to apply
   * @return the transformed mutable row schema.
   * @throws TransformationException
   */
  public static DefaultMutableRowSchema transformSchema(String table,
                                                        Schema schema,
                                                        Map<String, List<Transformation>> transformations)
    throws TransformationException {
    DefaultMutableRowSchema rowSchema = new DefaultMutableRowSchema(schema);
    for (String columnName : transformations.keySet()) {
      try {
        rowSchema = transformSchema(rowSchema, transformations.get(columnName));
      } catch (Exception e) {
        throw new TransformationException(table, columnName, e.getMessage(), e);
      }
    }
    return rowSchema;
  }

  /**
   * Apply a list of transformations on a row schema
   *
   * @param schema          the schema to apply transformation
   * @param transformations the list of transformations to apply
   * @return the transformed mutable row schema.
   */
  public static DefaultMutableRowSchema transformSchema(Schema schema, List<Transformation> transformations)
    throws Exception {
    DefaultMutableRowSchema rowSchema = new DefaultMutableRowSchema(schema);
    return transformSchema(rowSchema, transformations);
  }

  private static DefaultMutableRowSchema transformSchema(DefaultMutableRowSchema rowSchema,
    List<Transformation> transformations) throws Exception {
    for (Transformation transformation : transformations) {
      transformation.transformSchema(rowSchema);
    }
    return rowSchema;
  }

  /**
   * Apply a list of transformations on a DDLEvent
   * @param ddlEvent               the DDL event to apply transformations on
   * @param columnTransformations  the list of transformations to apply
   * @return the new DDL event that has applied the transformations specified
   */
  public static DDLEvent transformDDLEvent(DDLEvent ddlEvent, List<Transformation> columnTransformations)
    throws Exception {
    DefaultMutableRowSchema mutableRowSchema = TransformationUtil.transformSchema(
      ddlEvent.getSchema(), columnTransformations);
    ColumnRenameInfo renameInfo = mutableRowSchema.getRenameInfo();
    List<String> primaryKeys = ddlEvent.getPrimaryKey();
    List<String> changedKeys = new ArrayList<>(primaryKeys.size());
    for (String primaryKey : primaryKeys) {
      changedKeys.add(renameInfo.getNewName(primaryKey));
    }
    return DDLEvent.builder(ddlEvent).setSchema(mutableRowSchema.toSchema()).setPrimaryKey(changedKeys).build();
  }

  /**
   * Load transformation plugins applied on a certain table
   * @param configurer the plugin configurer
   * @param tableTransformations a map whose key is the table name and value is the table level transformation config
   *                            defined for that table
   * @param table the table that transformations are applied on
   * @return list of transformations applied on the specified table
   */
  public static Map<String, List<Transformation>> loadTransformations(Configurer configurer,
                                                         Map<String, TableTransformation> tableTransformations,
                                                         SourceTable table) throws TransformationException {
    String tableName = table.getSchema() == null ? table.getTable() : table.getSchema() + "." + table.getTable();
    Map<String, List<Transformation>> columnTransformations = new LinkedHashMap<>();
    TableTransformation tableTransformation = tableTransformations.get(tableName);
    if (tableTransformation == null) {
      return Collections.emptyMap();
    }
    for (ColumnTransformation t : tableTransformation.getColumnTransformations()) {
      String columnName = t.getColumnName();
      String directive = t.getDirective();
      String directiveName = TransformationUtil.parseDirectiveName(directive);
      try {
        Transformation transformation = configurer.usePlugin(Transformation.PLUGIN_TYPE, directiveName,
                                                             UUID.randomUUID().toString(),
                                                             PluginProperties.builder().build());
        transformation.initialize(new DefaultTransformationContext(directive));

        if (columnTransformations.containsKey(columnName)) {
          columnTransformations.getOrDefault(columnName, new ArrayList<>()).add(transformation);
        } else {
          columnTransformations.put(columnName, new ArrayList<>(Arrays.asList(transformation)));
        }
      } catch (Exception e) {
        throw new TransformationException(tableName, columnName, String.format("Exception while loading plugin " +
                                          "for directive : %s. Error : %s.", directive, e.getMessage()), e);
      }
    }
    return columnTransformations;
  }

  /**
   * Get table level transformation config for each table
   * @param deltaConfig the Delta App config
   * @return a map whose key is the table name and value is the table level transformation config defined for that table
   */
  public static Map<String, TableTransformation> getTableLevelTransformations(DeltaConfig deltaConfig) {
    return deltaConfig.getTableTransformations().stream().collect(
        Collectors.toMap(TableTransformation::getTableName, Function.identity()));
  }

}
