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

package io.cdap.delta.store;

import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.delta.api.DeltaPipelineId;
import io.cdap.delta.app.DeltaWorkerId;
import io.cdap.delta.app.OffsetAndSequence;

import java.util.Collection;

/**
 * Handles logic around storage and retrieval of offsets and other state store data.
 */
public class DBStateStoreService implements StateStore {
  private final TransactionRunner txRunner;

  public DBStateStoreService(TransactionRunner transactionRunner) {
    this.txRunner = transactionRunner;
  }

  @Override
  public OffsetAndSequence readOffset(DeltaWorkerId id) {
    return TransactionRunners.run(txRunner, context -> {
      return DBReplicationOffsetStore.get(context).getOffsets(id);
    });
  }

  @Override
  public void writeOffset(DeltaWorkerId id, OffsetAndSequence offset) {
    TransactionRunners.run(txRunner, context -> {
      DBReplicationOffsetStore.get(context).writeOffset(id, offset);
    });
  }

  @Override
  public void writeState(DeltaWorkerId id, String key, byte[] val) {
    TransactionRunners.run(txRunner, context -> {
      DBReplicationStateStore.get(context).writeStateData(id, key, val);
    });
  }

  @Override
  public byte[] readState(DeltaWorkerId id, String key) {
    return TransactionRunners.run(txRunner, context -> {
      return DBReplicationStateStore.get(context).getStateData(id, key);
    });
  }

  @Override
  public Long getLatestGeneration(String namespace, String pipelineName) {
    return TransactionRunners.run(txRunner, context -> {
      return DBReplicationStateStore.get(context).getLatestGeneration(namespace, pipelineName);
    });
  }

  @Override
  public Collection<Integer> getWorkerInstances(DeltaPipelineId pipelineId) {
    return TransactionRunners.run(txRunner, context -> {
      return DBReplicationStateStore.get(context).getWorkerInstances(pipelineId.getNamespace(),
                                                                     pipelineId.getApp(),
                                                                     pipelineId.getGeneration());
    });
  }
}
