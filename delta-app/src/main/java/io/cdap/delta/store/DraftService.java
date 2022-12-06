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

import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.assessment.PipelineAssessment;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableNotFoundException;
import io.cdap.delta.app.service.Assessor;
import io.cdap.delta.macros.MacroEvaluator;
import io.cdap.delta.proto.DBTable;
import io.cdap.delta.proto.DeltaConfig;
import io.cdap.delta.proto.DraftRequest;
import io.cdap.delta.proto.TableAssessmentResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Handles logic around storage, retrieval, and assessment of drafts.
 */
public class DraftService {
  private static final Logger LOG = LoggerFactory.getLogger(DraftService.class);
  private final TransactionRunner txRunner;
  private final MacroEvaluator macroEvaluator;
  private final Assessor assessor;

  public DraftService(TransactionRunner transactionRunner, MacroEvaluator macroEvaluator) {
    this.txRunner = transactionRunner;
    this.macroEvaluator = macroEvaluator;
    this.assessor = new Assessor();
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
    return listDraftTables(draftId, draft, configurer);
  }

  /**
   * List the database tables readable by the source in the given draft id. An instance of the plugin will be
   * instantiated in order to generate this list. This is an expensive operation.
   *
   * If the plugin cannot be instantiated, or if the table list cannot be generated due to missing information in
   * the draft, an {@link InvalidDraftException} will be thrown.
   *
   * @param draftId id of the draft
   * @param draft draft
   * @param configurer configurer used to instantiate plugins
   * @return list of tables readable by the source in the draft
   * @throws DraftNotFoundException if the draft does not exist
   * @throws InvalidDraftException if the table list cannot be fetched because the draft is invalid
   * @throws IOException if the was an IO error fetching the table list
   * @throws Exception if there was an error creating the table registry
   */
  public TableList listDraftTables(DraftId draftId, Draft draft, Configurer configurer) throws Exception {
    DeltaConfig config = draft.getConfig();
    Namespace namespace = draftId.getNamespace();
    config = macroEvaluator.evaluateMacros(namespace, config);
    return assessor.listTables(namespace.getName(), config, configurer);
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
   * @param dbTable the dbTable
   * @return list of tables readable by the source in the draft
   * @throws DraftNotFoundException if the draft does not exist
   * @throws InvalidDraftException if the table list cannot be fetched because the draft is invalid
   * @throws TableNotFoundException if the table does not exist
   * @throws IOException if the was an IO error fetching the table detail
   * @throws Exception if there was an error creating the table registry
   */
  public TableDetail describeDraftTable(DraftId draftId, Configurer configurer, DBTable dbTable) throws Exception {
    Draft draft = getDraft(draftId);
    DeltaConfig config = draft.getConfig();
    Namespace namespace = draftId.getNamespace();
    config = macroEvaluator.evaluateMacros(namespace, config);

    return assessor.describeTable(draftId.getNamespace().getName(), config, configurer, dbTable);
  }

  /**
   * Assess the given table detail using the plugins from the given draft. Plugins will be
   * instantiated in order to generate this assessment. This is an expensive operation.
   *
   * If a plugin cannot be instantiated due to missing draft information,
   * an {@link InvalidDraftException} will be thrown.
   *
   * @param draft id of the draft
   * @param dbTable the database, the schema, the table to be assessed
   * @param configurer configurer used to instantiate plugins
   * @return list of tables readable by the source in the draft
   * @throws DraftNotFoundException if the draft does not exist
   * @throws InvalidDraftException if the table list cannot be fetched because the draft is invalid
   * @throws TableNotFoundException if the table does not exist
   * @throws IOException if the table detail could not be read
   * @throws Exception if there was an error creating the table registry
   * @throws IllegalArgumentException if the table is not selected in the draft
   */
  public TableAssessmentResponse assessTable(Namespace namespace, Draft draft, Configurer configurer, DBTable dbTable)
    throws Exception {
    DeltaConfig config = draft.getConfig();
    return assessTable(namespace, config, configurer, dbTable);
  }

  /**
   * Assess the given table detail using the plugins from the given Delta Config. Plugins will be
   * instantiated in order to generate this assessment. This is an expensive operation.
   *
   * @param namespace namespace to look for macros
   * @param deltaConfig : delta config to assess
   * @param dbTable : the selected table to assess
   * @param configurer configurer used to instantiate plugins
   * @return list of tables readable by the source in the draft
   * @throws IOException if the table detail could not be read
   * @throws Exception if there was an error creating the table registry
   * @throws IllegalArgumentException if the number of tables is not equal to one.
   */
  public TableAssessmentResponse assessTable(Namespace namespace, DeltaConfig deltaConfig, Configurer configurer,
                                              DBTable dbTable) throws Exception {
    deltaConfig = macroEvaluator.evaluateMacros(namespace, deltaConfig);
    return assessor.assessTable(namespace.getName(), deltaConfig, configurer, dbTable);
  }

  /**
   * Assess the entire pipeline based on the tables the source will read and the capabilities of the target.
   * This will fetch draft from the store.
   */
  public PipelineAssessment assessPipeline(Namespace namespace, Draft draft, Configurer configurer) throws Exception {
    return assessPipeline(namespace, draft.getConfig(), configurer);
  }

  /**
   * Assess the entire pipeline based on the tables the source will read and the capabilities of the target.
   * An instance of the target plugin will be instantiated in order to generate this list.
   * This is an expensive operation.
   *
   * If the plugin cannot be instantiated due to missing draft information,
   * an {@link InvalidDraftException} will be thrown.
   *
   * @param namespace id of the draft
   * @param deltaConfig : delta config object to assess
   * @param configurer configurer used to instantiate plugins
   * @return list of tables readable by the source in the draft
   * @throws DraftNotFoundException if the draft does not exist
   * @throws InvalidDraftException if the table list cannot be fetched because the draft is invalid
   * @throws IOException if there was an IO error getting the list of source tables
   * @throws Exception if there was an error creating the table registry
   */
  public PipelineAssessment assessPipeline(Namespace namespace, DeltaConfig deltaConfig, Configurer configurer)
    throws Exception {
    deltaConfig = macroEvaluator.evaluateMacros(namespace, deltaConfig);
    return assessor.assessPipeline(namespace.getName(), deltaConfig, configurer);
  }
}
