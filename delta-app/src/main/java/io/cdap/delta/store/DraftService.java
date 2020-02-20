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

import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.SourceColumn;
import io.cdap.delta.api.SourceTable;
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
import io.cdap.delta.proto.DeltaConfig;
import io.cdap.delta.proto.DraftRequest;
import io.cdap.delta.proto.FullColumnAssessment;
import io.cdap.delta.proto.Plugin;
import io.cdap.delta.proto.Stage;
import io.cdap.delta.proto.TableAssessmentResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Handles logic around storage, retrieval, and assessment of drafts.
 */
public class DraftService {
  private final TransactionRunner txRunner;
  private final PropertyEvaluator propertyEvaluator;

  public DraftService(TransactionRunner transactionRunner, PropertyEvaluator propertyEvaluator) {
    this.txRunner = transactionRunner;
    this.propertyEvaluator = propertyEvaluator;
  }

  /**
   * List all drafts in the given namespace. If no drafts exist, an empty list is returned.
   *
   * @param namespace namespace to list drafts in
   * @return list of drafts in the namespace
   */
  public List<Draft> listDrafts(Namespace namespace) {
    return TransactionRunners.run(txRunner, context -> {
      return DraftStore.get(context).listDrafts(namespace);
    });
  }

  /**
   * Get the draft for the given id, or throw an exception if it does not exist.
   *
   * @param draftId the id of the draft
   * @return draft information
   * @throws DraftNotFoundException if the draft does not exist
   */
  public Draft getDraft(DraftId draftId) {
    Optional<Draft> draft = TransactionRunners.run(txRunner, context -> {
      return DraftStore.get(context).getDraft(draftId);
    });

    return draft.orElseThrow(() -> new DraftNotFoundException(draftId));
  }

  /**
   * Store the contents of a pipeline config as a draft. If a draft already exists, it's modified time is updated and
   * the config is overwritten. If none exists, a new draft is created.
   *
   * Throws an {@link InvalidDraftException} if the given DeltaConfig is invalid.
   *
   * @param draftId if of the draft to save
   * @param draft draft to save
   * @throws InvalidDraftException if the draft is invalid
   */
  public void saveDraft(DraftId draftId, DraftRequest draft) {
    try {
      draft.getConfig().validateDraft();
    } catch (IllegalArgumentException e) {
      throw new InvalidDraftException(e.getMessage(), e);
    }
    TransactionRunners.run(txRunner, context -> {
      DraftStore draftStore = DraftStore.get(context);
      draftStore.writeDraft(draftId, draft);
    });
  }

  /**
   * Delete the given draft.
   *
   * @param draftId id of the draft to delete
   */
  public void deleteDraft(DraftId draftId) {
    TransactionRunners.run(txRunner, context -> {
      DraftStore draftStore = DraftStore.get(context);
      draftStore.deleteDraft(draftId);
    });
  }

  /**
   * List the database tables readable by the source in the given draft id. An instance of the plugin will be
   * instantiated in order to generate this list. This is an expensive operation.
   *
   * If the plugin cannot be instantiated, or if the table list cannot be generated due to missing information in
   * the draft, an {@link InvalidDraftException} will be thrown.
   *
   * @param draftId id of the draft
   * @param configurer configurer used to instantiate plugins
   * @return list of tables readable by the source in the draft
   * @throws DraftNotFoundException if the draft does not exist
   * @throws InvalidDraftException if the table list cannot be fetched because the draft is invalid
   * @throws IOException if the was an IO error fetching the table list
   * @throws Exception if there was an error creating the table registry
   */
  public TableList listDraftTables(DraftId draftId, Configurer configurer) throws Exception {
    Draft draft = getDraft(draftId);
    try (TableRegistry tableRegistry = createTableRegistry(draftId, draft, configurer)) {
      return tableRegistry.listTables();
    }
  }

  /**
   * Describe the given database table using the source from the given draft id. An instance of the plugin will be
   * instantiated in order to generate this list. This is an expensive operation.
   *
   * If the plugin cannot be instantiated, or if the table cannot be described to missing information in
   * the draft, an {@link InvalidDraftException} will be thrown.
   *
   * @param draftId id of the draft
   * @param configurer configurer used to instantiate plugins
   * @param database the table database
   * @param table the table name
   * @return list of tables readable by the source in the draft
   * @throws DraftNotFoundException if the draft does not exist
   * @throws InvalidDraftException if the table list cannot be fetched because the draft is invalid
   * @throws TableNotFoundException if the table does not exist
   * @throws IOException if the was an IO error fetching the table detail
   * @throws Exception if there was an error creating the table registry
   */
  public TableDetail describeDraftTable(DraftId draftId, Configurer configurer, String database, String table)
    throws Exception {
    Draft draft = getDraft(draftId);
    try (TableRegistry tableRegistry = createTableRegistry(draftId, draft, configurer)) {
      return tableRegistry.describeTable(database, table);
    }
  }

  /**
   * Assess the given table detail using the plugins from the given draft. Plugins will be
   * instantiated in order to generate this assessment. This is an expensive operation.
   *
   * If a plugin cannot be instantiated due to missing draft information,
   * an {@link InvalidDraftException} will be thrown.
   *
   * @param draftId id of the draft
   * @param configurer configurer used to instantiate plugins
   * @param db the database the table lives in
   * @param table the table to assess
   * @return list of tables readable by the source in the draft
   * @throws DraftNotFoundException if the draft does not exist
   * @throws InvalidDraftException if the table list cannot be fetched because the draft is invalid
   * @throws TableNotFoundException if the table does not exist
   * @throws IOException if the table detail could not be read
   * @throws Exception if there was an error creating the table registry
   */
  public TableAssessmentResponse assessTable(DraftId draftId, Configurer configurer, String db, String table)
    throws Exception {
    Draft draft = getDraft(draftId);
    DeltaConfig deltaConfig = draft.getConfig();
    deltaConfig = evaluateMacros(draftId, deltaConfig);
    Stage target = deltaConfig.getTarget();
    if (target == null) {
      throw new InvalidDraftException("Cannot assess a table without a configured target.");
    }
    TableRegistry tableRegistry = createTableRegistry(draftId, draft, configurer);
    TableAssessor<TableDetail> sourceTableAssessor = createTableAssessor(configurer, deltaConfig.getSource());
    TableAssessor<StandardizedTableDetail> targetTableAssessor = createTableAssessor(configurer, target);
    return assessTable(new SourceTable(db, table), tableRegistry, sourceTableAssessor, targetTableAssessor);
  }

  /**
   * Assess the entire pipeline based on the tables the source will read and the capabilities of the target.
   * An instance of the target plugin will be instantiated in order to generate this list.
   * This is an expensive operation.
   *
   * If the plugin cannot be instantiated due to missing draft information,
   * an {@link InvalidDraftException} will be thrown.
   *
   * @param draftId id of the draft
   * @param configurer configurer used to instantiate plugins
   * @return list of tables readable by the source in the draft
   * @throws DraftNotFoundException if the draft does not exist
   * @throws InvalidDraftException if the table list cannot be fetched because the draft is invalid
   * @throws IOException if there was an IO error getting the list of source tables
   * @throws Exception if there was an error creating the table registry
   */
  public PipelineAssessment assessPipeline(DraftId draftId, Configurer configurer) throws Exception {
    Draft draft = getDraft(draftId);
    DeltaConfig deltaConfig = draft.getConfig();
    deltaConfig.validatePipeline();
    deltaConfig = evaluateMacros(draftId, deltaConfig);

    TableRegistry tableRegistry = createTableRegistry(draftId, draft, configurer);
    TableAssessor<TableDetail> sourceTableAssessor = createTableAssessor(configurer, deltaConfig.getSource());
    //noinspection ConstantConditions
    TableAssessor<StandardizedTableDetail> targetTableAssessor =
      createTableAssessor(configurer, deltaConfig.getTarget());

    List<Problem> connectivityIssues = new ArrayList<>();
    List<TableSummaryAssessment> tableAssessments = new ArrayList<>();

    // if no source tables are given, this means all tables should be read
    List<SourceTable> tablesToAssess = deltaConfig.getTables();
    if (tablesToAssess.isEmpty()) {
      tablesToAssess = tableRegistry.listTables().getTables().stream()
        .map(t -> new SourceTable(t.getDatabase(), t.getTable()))
        .collect(Collectors.toList());
    }

    // go through all tables that the pipeline should read, fetching detail about each of the tables
    for (SourceTable sourceTable : tablesToAssess) {
      String db = sourceTable.getDatabase();
      String table = sourceTable.getTable();
      try {
        TableAssessmentResponse assessment = assessTable(sourceTable, tableRegistry, sourceTableAssessor,
                                                         targetTableAssessor);
        tableAssessments.add(summarize(db, table, sourceTable.getSchema(), assessment));
      } catch (TableNotFoundException e) {
        connectivityIssues.add(
          new Problem("Table Not Found",
                      String.format("Table '%s' in database '%s' was not found.", table, db),
                      "Check the table information and permissions",
                      null));
      } catch (IOException e) {
        connectivityIssues.add(
          new Problem("Table Describe Error",
                      String.format("Unable to fetch details about table '%s' in database '%s': %s",
                                    table, db, e.getMessage()),
                      "Check permissions and database connectivity",
                      null));
      }
    }
    return new PipelineAssessment(tableAssessments, Collections.emptyList(), connectivityIssues);
  }

  private TableAssessmentResponse assessTable(SourceTable sourceTable, TableRegistry tableRegistry,
                                              TableAssessor<TableDetail> sourceTableAssessor,
                                              TableAssessor<StandardizedTableDetail> targetTableAssesor)
    throws IOException, TableNotFoundException {
    String db = sourceTable.getDatabase();
    String table = sourceTable.getTable();
    Set<String> columnWhitelist = sourceTable.getColumns().stream()
      .map(SourceColumn::getName)
      .collect(Collectors.toSet());

    // fetch detail about the table, then filter out columns that will not be read by the source
    TableDetail detail = tableRegistry.describeTable(db, table);
    List<ColumnDetail> selectedColumns = detail.getColumns().stream()
      // if there are no columns specified, it means all columns should be read
      .filter(columnWhitelist.isEmpty() ? col -> true : col -> columnWhitelist.contains(col.getName()))
      .collect(Collectors.toList());
    TableDetail filteredDetail = new TableDetail(db, table, detail.getSchema(), detail.getPrimaryKey(),
                                                 selectedColumns);
    TableAssessment srcAssessment = sourceTableAssessor.assess(filteredDetail);

    StandardizedTableDetail standardizedDetail = tableRegistry.standardize(filteredDetail);
    TableAssessment targetAssessment = targetTableAssesor.assess(standardizedDetail);

    return merge(srcAssessment, targetAssessment);
  }

  /**
   * Merge the assessment from the source and target into a single assessment.
   * This amounts to merging the assessment for each column.
   */
  private TableAssessmentResponse merge(TableAssessment srcAssessment, TableAssessment targetAssessment) {
    Map<String, ColumnAssessment> targetColumns = targetAssessment.getColumns().stream()
      .filter(c -> c.getSourceName() != null)
      .collect(Collectors.toMap(ColumnAssessment::getSourceName, c -> c));

    List<FullColumnAssessment> fullColumns = new ArrayList<>();
    Set<String> addedColumns = new HashSet<>();
    // add columns from the source
    for (ColumnAssessment srcColumn : srcAssessment.getColumns()) {
      String name = srcColumn.getName();
      ColumnAssessment targetColumn = targetColumns.get(name);
      fullColumns.add(merge(srcColumn, targetColumn));
      addedColumns.add(name);
    }
    // add columns present only in the target and not the source
    targetAssessment.getColumns().stream()
      .filter(t -> !addedColumns.contains(t.getName()))
      .forEach(t -> fullColumns.add(new FullColumnAssessment(t.getSupport(), null, null, t.getName(), t.getType(),
                                                             t.getSuggestion())));

    List<Problem> features = new ArrayList<>(srcAssessment.getFeatureProblems());
    features.addAll(targetAssessment.getFeatureProblems());
    return new TableAssessmentResponse(fullColumns, features);
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

  private TableRegistry createTableRegistry(DraftId id, Draft draft, Configurer configurer) throws Exception {
    DeltaConfig deltaConfig = draft.getConfig();
    deltaConfig = evaluateMacros(id, deltaConfig);
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

  private <T> TableAssessor<T> createTableAssessor(Configurer configurer, Stage stage) {
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
      return plugin.createTableAssessor(configurer);
    } catch (Exception e) {
      throw new InvalidDraftException(String.format("Unable to instantiate table assessor for stage '%s': %s",
                                                    stage.getName(), e.getMessage()), e);
    }
  }

  private TableSummaryAssessment summarize(String db, String table, @Nullable String schema,
                                           TableAssessmentResponse assessment) {
    int numUnsupported = 0;
    int numPartial = 0;
    int numColumns = 0;
    for (FullColumnAssessment columnAssessment : assessment.getColumns()) {
      if (columnAssessment.getSupport() == ColumnSupport.NO) {
        numUnsupported++;
      } else if (columnAssessment.getSupport() == ColumnSupport.PARTIAL) {
        numPartial++;
      }
      numColumns++;
    }
    return new TableSummaryAssessment(db, table, numColumns, numUnsupported, numPartial, schema);
  }

  private DeltaConfig evaluateMacros(DraftId draftId, DeltaConfig config) {
    DeltaConfig.Builder builder = DeltaConfig.builder()
      .setDescription(config.getDescription())
      .setResources(config.getResources())
      .setSource(config.getSource())
      .setOffsetBasePath(config.getOffsetBasePath())
      .setTables(config.getTables())
      .setSource(evaluateMacros(draftId.getNamespace().getName(), config.getSource()));

    Stage target = config.getTarget();
    if (target != null) {
      builder.setTarget(evaluateMacros(draftId.getNamespace().getName(), target));
    }

    return builder.build();
  }

  private Stage evaluateMacros(String namespace, Stage stage) {
    Plugin plugin = stage.getPlugin();
    Map<String, String> evaluatedProperties = propertyEvaluator.evaluate(namespace, plugin.getProperties());
    Plugin evaluatedPlugin = new Plugin(plugin.getName(), plugin.getType(), evaluatedProperties, plugin.getArtifact());
    return new Stage(stage.getName(), evaluatedPlugin);
  }
}
