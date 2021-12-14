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

/**
 *  An interface for state store responsible to store and read the replicator state data.
 */
public interface StateStore {

  OffsetAndSequence readOffset(DeltaWorkerId id) throws IOException;

  void writeOffset(DeltaWorkerId id, OffsetAndSequence offset) throws IOException;

  byte[] readState(DeltaWorkerId id, String key) throws IOException;

  void writeState(DeltaWorkerId id, String key, byte[] val) throws IOException;

  Long getLatestGeneration(String namespace, String pipelineName) throws IOException;

  Collection<Integer> getWorkerInstances(DeltaPipelineId pipelineId) throws IOException;
}
