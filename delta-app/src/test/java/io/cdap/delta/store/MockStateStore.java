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

import io.cdap.delta.api.DeltaPipelineId;
import io.cdap.delta.app.DeltaWorkerId;
import io.cdap.delta.app.OffsetAndSequence;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 *  A mock of statestore with stare as in-memory hashmaps
 */
public class MockStateStore implements StateStore {

  Map<DeltaWorkerId, OffsetAndSequence> inMemoryOffsetDbTable;
  Map<String, byte[]> inMemoryStateDbTable;


  public MockStateStore() {
    this.inMemoryOffsetDbTable = new HashMap<>();
    this.inMemoryStateDbTable = new HashMap<>();
  }

  @Override
  public OffsetAndSequence readOffset(DeltaWorkerId id) throws IOException {
    return inMemoryOffsetDbTable.getOrDefault(id, null);
  }

  @Override
  public void writeOffset(DeltaWorkerId id, OffsetAndSequence offset) throws IOException {
    inMemoryOffsetDbTable.put(id, offset);
  }

  @Override
  public byte[] readState(DeltaWorkerId id, String key) throws IOException {
    return inMemoryStateDbTable.getOrDefault(makeKey(id, key), new byte[0]);
  }

  @Override
  public void writeState(DeltaWorkerId id, String key, byte[] val) throws IOException {
    inMemoryStateDbTable.put(makeKey(id, key), val);
  }

  @Override
  public Long getLatestGeneration(String namespace, String pipelineName) throws IOException {
    return null;
  }

  @Override
  public Collection<Integer> getWorkerInstances(DeltaPipelineId pipelineId) throws IOException {
    return null;
  }

  protected String makeKey(DeltaWorkerId id, String key) {
    return String.format("%s/%s/%d/%d/%s",
                         id.getPipelineId().getNamespace(),
                         id.getPipelineId().getApp(),
                         id.getPipelineId().getGeneration(),
                         id.getInstanceId(),
                         key);
  }

}
