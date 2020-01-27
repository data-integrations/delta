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
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableNotFoundException;
import io.cdap.delta.api.assessment.TableRegistry;
import io.cdap.delta.proto.DeltaConfig;
import io.cdap.delta.proto.Plugin;
import io.cdap.delta.proto.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

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
    try (TableRegistry tableRegistry = createTableRegistry(draftId, configurer)) {
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
  public TableDetail describeDraftTable(DraftId draftId, Configurer configurer, String database, String table)
    throws IOException, TableNotFoundException {
    try (TableRegistry tableRegistry = createTableRegistry(draftId, configurer)) {
      return tableRegistry.describeTable(database, table);
    }
  }

  private TableRegistry createTableRegistry(DraftId draftId, Configurer configurer) {
    Draft draft = getDraft(draftId);
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

    return deltaSource.createTableRegistry(configurer);
  }
}
