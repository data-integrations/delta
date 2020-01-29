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

import io.cdap.cdap.api.data.schema.Schema;
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
import io.cdap.delta.api.assessment.PipelineAssessment;
import io.cdap.delta.api.assessment.Problem;
import io.cdap.delta.api.assessment.SourceTableDetail;
import io.cdap.delta.api.assessment.StandardizedTableDetail;
import io.cdap.delta.api.assessment.Support;
import io.cdap.delta.api.assessment.TableAssessment;
import io.cdap.delta.api.assessment.TableAssessor;
import io.cdap.delta.api.assessment.TableAssessorSupplier;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableNotFoundException;
import io.cdap.delta.api.assessment.TableRegistry;
import io.cdap.delta.api.assessment.TableSummaryAssessment;
import io.cdap.delta.proto.DeltaConfig;
import io.cdap.delta.proto.Plugin;
import io.cdap.delta.proto.Stage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Handles logic around storage, retrieval, and assessment of drafts.
 */
public class DraftService {
  private final TransactionRunner txRunner;

  public DraftService(TransactionRunner transactionRunner) {
    this.txRunner = transactionRunner;
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
   * @param config pipeline config to save as a draft
   * @throws InvalidDraftException if the draft is invalid
   */
  public void saveDraft(DraftId draftId, DeltaConfig config) {
    try {
      config.validateDraft();
    } catch (IllegalArgumentException e) {
      throw new InvalidDraftException(e.getMessage(), e);
    }
    TransactionRunners.run(txRunner, context -> {
      DraftStore draftStore = DraftStore.get(context);
      draftStore.writeDraft(draftId, config);
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
   */
  public TableList listDraftTables(DraftId draftId, Configurer configurer) throws IOException {
    Draft draft = getDraft(draftId);
    try (TableRegistry tableRegistry = createTableRegistry(draft, configurer)) {
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
   */
  public TableDetail<List<ColumnDetail>> describeDraftTable(DraftId draftId, Configurer configurer,
                                                            String database, String table)
    throws IOException, TableNotFoundException {
    Draft draft = getDraft(draftId);
    try (TableRegistry tableRegistry = createTableRegistry(draft, configurer)) {
      return tableRegistry.describeTable(database, table);
    }
  }

  /**
   * Assess the given table detail using the target from the given draft id. An instance of the plugin will be
   * instantiated in order to generate this list. This is an expensive operation.
   *
   * If the plugin cannot be instantiated due to missing draft information,
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
   */
  public TableAssessment assessTable(DraftId draftId, Configurer configurer, String db, String table)
    throws IOException, TableNotFoundException {
    Draft draft = getDraft(draftId);
    DeltaConfig deltaConfig = draft.getConfig();
    TableRegistry tableRegistry = createTableRegistry(draft, configurer);
    TableAssessor<List<ColumnDetail>> sourceTableAssessor = createTableAssessor(configurer, deltaConfig.getSource());
    TableAssessor<Schema> targetTableAssessor = createTableAssessor(configurer, deltaConfig.getTarget());
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
   */
  public PipelineAssessment assessPipeline(DraftId draftId, Configurer configurer) {
    Draft draft = getDraft(draftId);
    DeltaConfig deltaConfig = draft.getConfig();
    deltaConfig.validatePipeline();

    TableRegistry tableRegistry = createTableRegistry(draft, configurer);
    TableAssessor<List<ColumnDetail>> sourceTableAssessor = createTableAssessor(configurer, deltaConfig.getSource());
    TableAssessor<Schema> targetTableAssessor = createTableAssessor(configurer, deltaConfig.getTarget());

    List<Problem> connectivityIssues = new ArrayList<>();
    List<TableSummaryAssessment> tableAssessments = new ArrayList<>();
    // go through all tables that the pipeline should read, fetching detail about each of the tables
    for (SourceTable sourceTable : draft.getConfig().getTables()) {
      String db = sourceTable.getDatabase();
      String table = sourceTable.getTable();
      try {
        TableAssessment assessment = assessTable(sourceTable, tableRegistry, sourceTableAssessor, targetTableAssessor);
        tableAssessments.add(summarize(db, table, assessment));
      } catch (TableNotFoundException e) {
        connectivityIssues.add(
          new Problem("Table Not Found",
                      String.format("Table '%s' in database '%s' was not found.", db, table),
                      "Check the table information and permissions",
                      null));
      } catch (IOException e) {
        connectivityIssues.add(
          new Problem("Table Describe Error",
                      String.format("Unable to fetch details about table '%s' in database '%s': %s",
                                    db, table, e.getMessage()),
                      "Check permissions and database connectivity",
                      null));
      }
    }
    return new PipelineAssessment(tableAssessments, Collections.emptyList(), connectivityIssues);
  }

  private TableAssessment assessTable(SourceTable sourceTable, TableRegistry tableRegistry,
                                      TableAssessor<List<ColumnDetail>> sourceTableAssessor,
                                      TableAssessor<Schema> targetTableAssesor)
    throws IOException, TableNotFoundException {
    String db = sourceTable.getDatabase();
    String table = sourceTable.getTable();
    Set<String> columnWhitelist = sourceTable.getColumns().stream()
      .map(SourceColumn::getName)
      .collect(Collectors.toSet());

    // fetch detail about the table, then filter out columns that will not be read by the source
    SourceTableDetail detail = tableRegistry.describeTable(db, table);
    List<ColumnDetail> selectedColumns = detail.getColumns().stream()
      // if there are no columns specified, it means all columns should be read
      .filter(columnWhitelist.isEmpty() ? col -> true : col -> columnWhitelist.contains(col.getName()))
      .collect(Collectors.toList());
    SourceTableDetail filteredDetail = new SourceTableDetail(db, table, detail.getPrimaryKey(), selectedColumns);
    TableAssessment srcAssessment = sourceTableAssessor.assess(filteredDetail);

    Schema standardSchema = tableRegistry.standardizeSchema(filteredDetail.getColumns());
    StandardizedTableDetail standardizedDetail = new StandardizedTableDetail(db, table, detail.getPrimaryKey(),
                                                                             standardSchema);
    TableAssessment targetAssessment = targetTableAssesor.assess(standardizedDetail);

    return merge(srcAssessment, targetAssessment);
  }

  private TableAssessment merge(TableAssessment srcAssessment, TableAssessment targetAssessment) {
    List<ColumnAssessment> columnAssessments = new ArrayList<>();
    Iterator<ColumnAssessment> targetColumnAssessments = targetAssessment.getColumns().iterator();
    for (ColumnAssessment columnAssessment : srcAssessment.getColumns()) {
      columnAssessments.add(merge(columnAssessment, targetColumnAssessments.next()));
    }
    return new TableAssessment(columnAssessments);
  }

  private ColumnAssessment merge(ColumnAssessment srcAssessment, ColumnAssessment targetAssessment) {
    if (srcAssessment.getSupport() == Support.NO) {
      return srcAssessment;
    } else if (targetAssessment.getSupport() == Support.NO) {
      return targetAssessment;
    } else if (srcAssessment.getSupport() == Support.PARTIAL) {
      return srcAssessment;
    } else if (targetAssessment.getSupport() == Support.PARTIAL) {
      return targetAssessment;
    }
    return targetAssessment;
  }

  private TableRegistry createTableRegistry(Draft draft, Configurer configurer) {
    DeltaConfig deltaConfig = draft.getConfig();
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

    return new ValidatingTableRegistry(deltaSource.createTableRegistry(configurer));
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

  private TableSummaryAssessment summarize(String db, String table, TableAssessment assessment) {
    int numUnsupported = 0;
    int numPartial = 0;
    int numColumns = 0;
    for (ColumnAssessment columnAssessment : assessment.getColumns()) {
      if (columnAssessment.getSupport() == Support.NO) {
        numUnsupported++;
      } else if (columnAssessment.getSupport() == Support.PARTIAL) {
        numPartial++;
      }
      numColumns++;
    }
    return new TableSummaryAssessment(db, table, numColumns, numUnsupported, numPartial);
  }
}
