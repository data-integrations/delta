/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.delta.app.service;

import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.SourceColumn;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.api.assessment.Assessment;
import io.cdap.delta.api.assessment.ColumnAssessment;
import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.ColumnSuggestion;
import io.cdap.delta.api.assessment.ColumnSupport;
import io.cdap.delta.api.assessment.PipelineAssessment;
import io.cdap.delta.api.assessment.Problem;
import io.cdap.delta.api.assessment.StandardizedTableDetail;
import io.cdap.delta.api.assessment.TableAssessment;
import io.cdap.delta.api.assessment.TableAssessor;
import io.cdap.delta.api.assessment.TableAssessorSupplier;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableNotFoundException;
import io.cdap.delta.api.assessment.TableRegistry;
import io.cdap.delta.api.assessment.TableSummaryAssessment;
import io.cdap.delta.proto.DBTable;
import io.cdap.delta.proto.DeltaConfig;
import io.cdap.delta.proto.FullColumnAssessment;
import io.cdap.delta.proto.Plugin;
import io.cdap.delta.proto.Stage;
import io.cdap.delta.proto.TableAssessmentResponse;
import io.cdap.delta.proto.TableTransformation;
import io.cdap.delta.store.DraftNotFoundException;
import io.cdap.delta.store.InvalidDraftException;
import io.cdap.transformation.ColumnRenameInfo;
import io.cdap.transformation.DefaultMutableRowSchema;
import io.cdap.transformation.DefaultRenameInfo;
import io.cdap.transformation.TransformationException;
import io.cdap.transformation.TransformationUtil;
import io.cdap.transformation.api.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Handles logic around assessment of delta config.
 */
public class Assessor {
  private static final Logger LOG = LoggerFactory.getLogger(Assessor.class);

  /**
   * List the database tables readable by the source in the given delta config. An instance of the plugin will be
   * instantiated in order to generate this list. This is an expensive operation.
   * <p>
   * If the plugin cannot be instantiated, or if the table list cannot be generated due to missing information in
   * the draft, an {@link InvalidDraftException} will be thrown.
   *
   * @param namespace  namespace
   * @param config     {@link DeltaConfig}
   * @param configurer configurer used to instantiate plugins
   * @return list of tables readable by the source in the draft
   * @throws DraftNotFoundException if the draft does not exist
   * @throws InvalidDraftException  if the table list cannot be fetched because the delta config is invalid
   * @throws IOException            if the was an IO error fetching the table list
   * @throws Exception              if there was an error creating the table registry
   */
  public TableList listTables(String namespace, DeltaConfig config, Configurer configurer) throws Exception {
    try (TableRegistry tableRegistry = createTableRegistry(namespace, config, configurer)) {
      return tableRegistry.listTables();
    }
  }

  /**
   * Describe the given database table using the source from the given delta config. An instance of the plugin will be
   * instantiated in order to generate this list. This is an expensive operation.
   * <p>
   * If the plugin cannot be instantiated, or if the table cannot be described to missing information in
   * the draft, an {@link InvalidDraftException} will be thrown.
   *
   * @param namespace  namespace
   * @param config     {@link DeltaConfig}
   * @param configurer configurer used to instantiate plugins
   * @param dbTable    the dbTable
   * @return list of tables readable by the source in the draft
   * @throws DraftNotFoundException if the draft does not exist
   * @throws InvalidDraftException  if the table list cannot be fetched because the deltaconfig is invalid
   * @throws TableNotFoundException if the table does not exist
   * @throws IOException            if the was an IO error fetching the table detail
   * @throws Exception              if there was an error creating the table registry
   */
  public TableDetail describeTable(String namespace, DeltaConfig config, Configurer configurer, DBTable dbTable)
    throws Exception {
    try (TableRegistry tableRegistry = createTableRegistry(namespace, config, configurer)) {
      if (dbTable.getSchema() == null) {
        return tableRegistry.describeTable(dbTable.getDatabase(), dbTable.getTable());
      } else {
        return tableRegistry.describeTable(dbTable.getDatabase(), dbTable.getSchema(), dbTable.getTable());
      }
    }
  }

  /**
   * Assess the given table detail using the plugins from the given Delta Config. Plugins will be
   * instantiated in order to generate this assessment. This is an expensive operation.
   *
   * @param namespace   namespace to look for macros
   * @param deltaConfig : delta config to assess
   * @param dbTable     : the selected table to assess
   * @param configurer  configurer used to instantiate plugins
   * @return list of tables readable by the source in the draft
   * @throws IOException              if the table detail could not be read
   * @throws Exception                if there was an error creating the table registry
   * @throws IllegalArgumentException if the number of tables is not equal to one.
   */
  public TableAssessmentResponse assessTable(String namespace, DeltaConfig deltaConfig, Configurer configurer,
                                             DBTable dbTable) throws Exception {
    SourceTable selectedTable = deltaConfig.getTables().stream().filter(
        streamTable -> dbTable.getDatabase().equals(streamTable.getDatabase()) &&
          dbTable.getTable().equals(streamTable.getTable()) &&
          (dbTable.getSchema() == streamTable.getSchema() || dbTable.getSchema() != null
            && dbTable.getSchema().equals(streamTable.getSchema()))).findAny()
      .orElseThrow(() -> new IllegalArgumentException(String
                                                        .format("Table '%s' in database '%s' and schema '%s' is not a" +
                                                                  " selected table in the draft", dbTable.getTable(),
                                                                dbTable.getDatabase(), dbTable.getSchema())));
    Stage target = deltaConfig.getTarget();
    if (target == null) {
      throw new InvalidDraftException("Cannot assess a table without a configured target.");
    }

    Map<String, TableTransformation> tableLevelTransformations =
      TransformationUtil.getTableLevelTransformations(deltaConfig);
    Map<String, List<Transformation>> transformations = new HashMap<>();
    try {
      transformations = TransformationUtil.loadTransformations(configurer, tableLevelTransformations, selectedTable);
    } catch (TransformationException e) {
      LOG.error("Failed to apply transformation: ", e);
      List<Problem> transformationErrors = new ArrayList<>();
      transformationErrors.add(new Problem("Transformation Loading failed", e.getMessage(),
                                           "Please ensure the applied transformation's plugin is uploaded'",
                                           "", Problem.Severity.ERROR, e.getTableName(), e.getColumnName()));

      return new TableAssessmentResponse(Collections.emptyList(), Collections.emptyList(), transformationErrors);
    }

    try (TableRegistry tableRegistry = createTableRegistry(namespace, deltaConfig, configurer);
         TableAssessor<TableDetail> sourceTableAssessor = createTableAssessor(configurer, deltaConfig.getSource(),
                                                                              deltaConfig.getTables());
         TableAssessor<StandardizedTableDetail> targetTableAssessor = createTableAssessor(configurer, target,
                                                                                          deltaConfig.getTables())) {

      TableAssessmentResponse assessment = assessTable(selectedTable, tableRegistry, sourceTableAssessor,
                                                       targetTableAssessor, transformations);
      return assessment;
    }
  }

  /**
   * Assess the entire pipeline based on the tables the source will read and the capabilities of the target.
   * An instance of the target plugin will be instantiated in order to generate this list.
   * This is an expensive operation.
   * <p>
   * If the plugin cannot be instantiated due to missing properties in delta config,
   * an {@link InvalidDraftException} will be thrown.
   *
   * @param namespace   id of the draft
   * @param deltaConfig : delta config object to assess
   * @param configurer  configurer used to instantiate plugins
   * @return list of tables readable by the source in the draft
   * @throws DraftNotFoundException if the draft does not exist
   * @throws InvalidDraftException  if the table list cannot be fetched because the draft is invalid
   * @throws IOException            if there was an IO error getting the list of source tables
   * @throws Exception              if there was an error creating the table registry
   */
  public PipelineAssessment assessPipeline(String namespace, DeltaConfig deltaConfig, Configurer configurer)
    throws Exception {
    try (TableRegistry tableRegistry = createTableRegistry(namespace, deltaConfig, configurer);
         TableAssessor<TableDetail> sourceTableAssessor = createTableAssessor(configurer, deltaConfig.getSource(),
                                                                              deltaConfig.getTables());
         TableAssessor<StandardizedTableDetail> targetTableAssessor =
           createTableAssessor(configurer, deltaConfig.getTarget(), deltaConfig.getTables())) {

      List<Problem> missingFeatures = new ArrayList<>();
      List<Problem> connectivityIssues = new ArrayList<>();
      List<Problem> transformationIssues = new ArrayList<>();
      List<TableSummaryAssessment> tableAssessments = new ArrayList<>();

      Assessment sourceAssessment = sourceTableAssessor.assess();
      Assessment targetAssessment = targetTableAssessor.assess();
      missingFeatures.addAll(sourceAssessment.getFeatures());
      missingFeatures.addAll(targetAssessment.getFeatures());
      connectivityIssues.addAll(sourceAssessment.getConnectivity());
      connectivityIssues.addAll(targetAssessment.getConnectivity());

      // if no source tables are given, this means all tables should be read
      List<SourceTable> tablesToAssess = deltaConfig.getTables();
      if (tablesToAssess.isEmpty()) {
        tablesToAssess = tableRegistry.listTables().getTables().stream()
          .map(t -> new SourceTable(t.getDatabase(), t.getTable()))
          .collect(Collectors.toList());
      }

      Map<String, TableTransformation> tableLevelTransformations =
        TransformationUtil.getTableLevelTransformations(deltaConfig);
      // go through all tables that the pipeline should read, fetching detail about each of the tables
      for (SourceTable sourceTable : tablesToAssess) {
        String db = sourceTable.getDatabase();
        String table = sourceTable.getTable();
        try {
          Map<String, List<Transformation>> transformations = TransformationUtil.loadTransformations(configurer,
            tableLevelTransformations, sourceTable);
          TableAssessmentResponse assessment = assessTable(sourceTable, tableRegistry, sourceTableAssessor,
            targetTableAssessor, transformations);
          missingFeatures.addAll(assessment.getFeatures());
          tableAssessments.add(summarize(db, sourceTable.getSchema(), sourceTable, assessment));
          transformationIssues.addAll(assessment.getTransformationIssues());
        } catch (TableNotFoundException e) {
          LOG.error(String.format("Table '%s' in database '%s' was not found.", table, db), e);
          connectivityIssues.add(
            new Problem("Table Not Found",
                        String.format("Table '%s' in database '%s' was not found.", table, db),
                        "Check the table information and permissions",
                        null));
        } catch (IOException e) {
          LOG.error(String.format("Unable to fetch details about table '%s' in database '%s'",
                  table, db), e);
          connectivityIssues.add(
            new Problem("Table Describe Error",
                        String.format("Unable to fetch details about table '%s' in database '%s': %s",
                                      table, db, e.getMessage()),
                        "Check permissions and database connectivity",
                        null));
        } catch (TransformationException e) {
          LOG.error("Failed to apply transformation: ", e);
          transformationIssues.add(new Problem("Transformation Loading failed", e.getMessage(),
                                               "Please ensure the applied transformation's plugin is uploaded'",
                                               "", Problem.Severity.ERROR, e.getTableName(), e.getColumnName()));
        }
      }
      return new PipelineAssessment(tableAssessments, missingFeatures, connectivityIssues, transformationIssues);
    }
  }

  private TableAssessmentResponse assessTable(SourceTable sourceTable, TableRegistry tableRegistry,
                                              TableAssessor<TableDetail> sourceTableAssessor,
                                              TableAssessor<StandardizedTableDetail> targetTableAssesor,
                                              Map<String, List<Transformation>> transformations)
    throws IOException, TableNotFoundException {
    String db = sourceTable.getDatabase();
    String table = sourceTable.getTable();
    String schema = sourceTable.getSchema();
    Set<String> columnWhitelist = sourceTable.getColumns().stream()
      .map(SourceColumn::getName)
      .collect(Collectors.toSet());

    // fetch detail about the table, then filter out columns that will not be read by the source
    TableDetail detail;
    if (schema == null) {
      detail = tableRegistry.describeTable(db, table);
    } else {
      detail = tableRegistry.describeTable(db, schema, table);
    }

    List<Problem> missingFeatures = new ArrayList<>(detail.getFeatures());
    List<String> unselectedPrimaryKeys = detail.getPrimaryKey().stream()
      // if there are no columns specified, it means all columns of the table are selected
      .filter(columnWhitelist.isEmpty() ? col -> false : col -> !columnWhitelist.contains(col))
      .collect(Collectors.toList());
    if (unselectedPrimaryKeys.size() == 1) {
      missingFeatures.add(
        new Problem("Missing Primary Key",
                    String.format("Column '%s' is part of the primary key for table '%s' in database '%s', " +
                                    "but is not selected to be replicated", unselectedPrimaryKeys.get(0),
                                  table, db),
                    "Please make sure this column been selected",
                    "This can result in different data at the target than at the source"));
    } else if (unselectedPrimaryKeys.size() > 1) {
      missingFeatures.add(
        new Problem("Missing Primary Key",
                    String.format("Columns '%s' are part of the primary key for table '%s' in database '%s', " +
                                    "but are not selected to be replicated",
                                  String.join(",", unselectedPrimaryKeys),
                                  table, db),
                    "Please make sure columns been selected",
                    "This can result in different data at the target than at the source"));
    }

    List<ColumnDetail> selectedColumns = detail.getColumns().stream()
      // if there are no columns specified, it means all columns should be read
      .filter(columnWhitelist.isEmpty() ? col -> true : col -> columnWhitelist.contains(col.getName()))
      .collect(Collectors.toList());
    TableDetail filteredDetail = TableDetail.builder(db, table, schema)
      .setPrimaryKey(detail.getPrimaryKey())
      .setColumns(selectedColumns)
      .setFeatures(missingFeatures)
      .build();
    TableAssessment srcAssessment = sourceTableAssessor.assess(filteredDetail);

    StandardizedTableDetail standardizedDetail = tableRegistry.standardize(filteredDetail);

    ColumnRenameInfo columnRenameInfo = new DefaultRenameInfo(Collections.emptyMap());
    List<Problem> transformationIssues = new ArrayList<>();
    if (!transformations.isEmpty()) {
      try {
        DefaultMutableRowSchema rowSchema = TransformationUtil.transformSchema(table, standardizedDetail.getSchema(),
                                                                               transformations);
        standardizedDetail = new StandardizedTableDetail(standardizedDetail.getDatabase(),
                                                         standardizedDetail.getTable(),
                                                         standardizedDetail.getPrimaryKey(),
                                                         rowSchema.toSchema());
        columnRenameInfo = rowSchema.getRenameInfo();
      } catch (TransformationException e) {
        LOG.error("Failed to apply transformation: ", e);
        transformationIssues.add(new Problem("Transformation failed", e.getMessage(),
                                             "Please ensure the applied transformation is valid",
                                             "The job cannot be deployed with invalid transformations",
                                             Problem.Severity.ERROR,
                                             e.getTableName(), e.getColumnName()));
      }
    }
    TableAssessment targetAssessment = targetTableAssesor.assess(standardizedDetail);

    return merge(srcAssessment, targetAssessment, columnRenameInfo, transformationIssues);
  }

  /**
   * Merge the assessment from the source and target into a single assessment.
   * This amounts to merging the assessment for each column.
   */
  private TableAssessmentResponse merge(TableAssessment srcAssessment, TableAssessment targetAssessment,
                                        ColumnRenameInfo columnRenameInfo, List<Problem> transformationIssues) {
    Map<String, ColumnAssessment> targetColumns = targetAssessment.getColumns().stream()
      .filter(c -> c.getSourceName() != null)
      .collect(Collectors.toMap(ColumnAssessment::getSourceName, c -> c));

    List<FullColumnAssessment> fullColumns = new ArrayList<>();
    Set<String> addedColumns = new HashSet<>();
    // add columns from the source
    for (ColumnAssessment srcColumn : srcAssessment.getColumns()) {
      String name = columnRenameInfo.getNewName(srcColumn.getName());
      ColumnAssessment targetColumn = targetColumns.get(name);
      if (targetColumn != null) {
        fullColumns.add(merge(srcColumn, targetColumn));
      } else {
        fullColumns.add(new FullColumnAssessment(srcColumn.getSupport(), srcColumn.getName(), srcColumn.getType(),
                                                 null, null, srcColumn.getSuggestion()));
      }
      addedColumns.add(name);
    }
    // add columns present only in the target and not the source
    targetAssessment.getColumns().stream()
      .filter(t -> !addedColumns.contains(t.getName()) && !addedColumns.contains(t.getSourceName()))
      .forEach(t -> fullColumns.add(new FullColumnAssessment(t.getSupport(), null, null, t.getName(), t.getType(),
                                                             t.getSuggestion())));

    List<Problem> features = new ArrayList<>(srcAssessment.getFeatureProblems());
    features.addAll(targetAssessment.getFeatureProblems());
    return new TableAssessmentResponse(fullColumns, features, transformationIssues);
  }

  /**
   * Merge the assessment form the source and target into a single assessment.
   * If one plugin supports a column but the other does not, that column is not supported from a pipeline perspective.
   * Similarly if one plugin fully supports a column but the other only partially supports it, that column is
   * partially supported from a pipeline perspective.
   * If both plugins do not support the column, or both partially support the column, the assessment from the source
   * is taken, because that is what needs to be addressed first.
   */
  private FullColumnAssessment merge(ColumnAssessment srcAssessment, ColumnAssessment targetAssessment) {
    ColumnSupport support = ColumnSupport.YES;
    if (srcAssessment.getSupport() == ColumnSupport.NO || targetAssessment.getSupport() == ColumnSupport.NO) {
      support = ColumnSupport.NO;
    } else if (srcAssessment.getSupport() == ColumnSupport.PARTIAL ||
      targetAssessment.getSupport() == ColumnSupport.PARTIAL) {
      support = ColumnSupport.PARTIAL;
    }
    ColumnSuggestion suggestion = srcAssessment.getSuggestion();
    if (suggestion == null) {
      suggestion = targetAssessment.getSuggestion();
    }
    return new FullColumnAssessment(support, srcAssessment.getName(), srcAssessment.getType(),
                                    targetAssessment.getName(), targetAssessment.getType(),
                                    suggestion);
  }

  private TableRegistry createTableRegistry(String namespace, DeltaConfig deltaConfig, Configurer configurer)
    throws Exception {
    Stage stage = deltaConfig.getSource();

    Plugin pluginConfig = stage.getPlugin();
    DeltaSource deltaSource;
    try {
      deltaSource = configurer.usePlugin(pluginConfig.getType(), pluginConfig.getName(), UUID.randomUUID().toString(),
                                         PluginProperties.builder().addAll(pluginConfig.getProperties()).build());
    } catch (InvalidPluginConfigException e) {
      throw new InvalidDraftException(String.format("Unable to instantiate source plugin: %s", e.getMessage()), e);
    }

    if (deltaSource == null) {
      throw new InvalidDraftException(String.format("Unable to find plugin '%s'", pluginConfig.getName()));
    }
    return deltaSource.createTableRegistry(configurer);
  }

  private <T> TableAssessor<T> createTableAssessor(Configurer configurer, Stage stage, List<SourceTable> tables) {
    Plugin pluginConfig = stage.getPlugin();
    TableAssessorSupplier<T> plugin;
    try {
      plugin = configurer.usePlugin(pluginConfig.getType(), pluginConfig.getName(), UUID.randomUUID().toString(),
                                    PluginProperties.builder().addAll(pluginConfig.getProperties()).build());
    } catch (InvalidPluginConfigException e) {
      throw new InvalidDraftException(
        String.format("Unable to instantiate plugin for stage '%s': %s", stage.getName(), e.getMessage()), e);
    }

    if (plugin == null) {
      throw new InvalidDraftException(String.format("Unable to find plugin '%s' for stage '%s'",
                                                    pluginConfig.getName(), stage.getName()));
    }

    try {
      return plugin.createTableAssessor(configurer, tables);
    } catch (Exception e) {
      throw new InvalidDraftException(String.format("Unable to instantiate table assessor for stage '%s': %s",
                                                    stage.getName(), e.getMessage()), e);
    }
  }

  private TableSummaryAssessment summarize(String db, @Nullable String schema, SourceTable table,
                                           TableAssessmentResponse assessment) {
    int numUnsupported = 0;
    int numPartial = 0;
    int numColumns = 0;
    for (FullColumnAssessment columnAssessment : assessment.getColumns()) {
      if (columnAssessment.getSupport() == ColumnSupport.NO) {
        numUnsupported++;
      } else if (columnAssessment.getSupport() == ColumnSupport.PARTIAL) {
        Set<SourceColumn> columns = table.getColumns();
        SourceColumn existingColumn = columns.stream()
          .filter(column -> columnAssessment.getSourceName().equals(column.getName()))
          .findAny()
          .orElse(null);

        if (existingColumn == null || !existingColumn.isSuppressWarning()) {
          numPartial++;
        }
      }
      numColumns++;
    }
    return new TableSummaryAssessment(db, table.getTable(), numColumns, numUnsupported, numPartial, schema);
  }
}
