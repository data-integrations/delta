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
import io.cdap.delta.proto.DeltaConfig;

import java.util.List;
import java.util.Optional;

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
  public Draft getDraft(DraftId draftId) throws DraftNotFoundException {
    Optional<Draft> draft = TransactionRunners.run(txRunner, context -> {
      return DraftStore.get(context).getDraft(draftId);
    });

    return draft.orElseThrow(() -> new DraftNotFoundException(draftId));
  }

  /**
   * Store the contents of a pipeline config as a draft. If a draft already exists, it's modified time is updated and
   * the config is overwritten. If none exists, a new draft is created.
   *
   * @param draftId if of the draft to save
   * @param config pipeline config to save as a draft
   */
  public void saveDraft(DraftId draftId, DeltaConfig config) {
    TransactionRunners.run(txRunner, context -> {
      DraftStore draftStore = DraftStore.get(context);
      draftStore.writeDraft(draftId, config);
    });
  }
}
