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

package io.cdap.delta.app;

import com.google.gson.Gson;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.delta.api.ReplicationError;
import io.cdap.delta.proto.DBTable;
import io.cdap.delta.proto.PipelineReplicationState;
import io.cdap.delta.proto.PipelineState;
import io.cdap.delta.proto.TableReplicationState;
import io.cdap.delta.proto.TableState;
import io.cdap.delta.store.StateStore;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Stores information about pipeline and table state. Stores state in memory, persisting it to the StateStore if
 * anything changes. Separate instances of this class should not be used to write state, as it will result in
 * inconsistent results due to the fact that it keeps state information in memory.
 */
public class PipelineStateService {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineStateService.class);
  private static final Gson GSON = new Gson();
  private static final String STATE_KEY = "pipeline";
  private final DeltaPipelineId pipelineId;
  private final StateStore stateStore;
  private final Map<DBTable, TableReplicationState> tables;
  private final RetryPolicy<Object> saveRetryPolicy;
  private PipelineState sourceState;
  private ReplicationError sourceError;

  public PipelineStateService(DeltaPipelineId pipelineId, StateStore stateStore) {
    this.pipelineId = pipelineId;
    this.stateStore = stateStore;
    this.tables = new HashMap<>();
    this.saveRetryPolicy = new RetryPolicy<>()
      .withMaxAttempts(Integer.MAX_VALUE)
      .withBackoff(1, 60, ChronoUnit.SECONDS)
      .onFailedAttempt(failure -> {
        // log the first time and in 1 minute increments to avoid spamming the log.
        if (failure.getAttemptCount() == 1 || !failure.getElapsedTime().minusMinutes(1).isNegative()) {
          LOG.warn("Unable to save pipeline replication state. This operation will be retried until it succeeds.",
                   failure.getLastFailure());
        }
      });
  }

  /**
   * Load state from persistent storage into memory.
   */
  public void load() throws IOException {
    byte[] bytes = stateStore.readState(pipelineId, STATE_KEY);
    if (bytes == null) {
      sourceState = PipelineState.OK;
      sourceError = null;
      tables.clear();
    } else {
      PipelineReplicationState replState = GSON.fromJson(Bytes.toString(bytes), PipelineReplicationState.class);
      sourceState = replState.getSourceState();
      sourceError = replState.getSourceError();
      tables.putAll(replState.getTables().stream()
                      .collect(Collectors.toMap(t -> new DBTable(t.getDatabase(), t.getTable()), t -> t)));
    }
  }

  public PipelineReplicationState getState() {
    return new PipelineReplicationState(sourceState, new HashSet<>(tables.values()), sourceError);
  }

  public synchronized void setSourceError(ReplicationError error) {
    setSourceState(PipelineState.ERROR, error);
  }

  public synchronized void setSourceOK() {
    setSourceState(PipelineState.OK, null);
  }

  public synchronized void setTableSnapshotting(DBTable dbTable) {
    setTableState(dbTable, new TableReplicationState(dbTable.getDatabase(), dbTable.getTable(),
                                                     TableState.SNAPSHOT, null));
  }

  public synchronized void setTableReplicating(DBTable dbTable) {
    setTableState(dbTable, new TableReplicationState(dbTable.getDatabase(), dbTable.getTable(),
                                                     TableState.REPLICATE, null));
  }

  public synchronized void setTableError(DBTable dbTable, ReplicationError error) {
    setTableState(dbTable, new TableReplicationState(dbTable.getDatabase(), dbTable.getTable(),
                                                     TableState.ERROR, error));
  }

  public synchronized void dropTable(DBTable dbTable) {
    if (tables.remove(dbTable) != null) {
      save();
    }
  }

  private void setSourceState(PipelineState state, ReplicationError error) {
    boolean shouldSave = sourceState != state;
    sourceState = state;
    sourceError = error;
    if (shouldSave) {
      save();
    }
  }

  private void setTableState(DBTable dbTable, TableReplicationState newState) {
    TableReplicationState oldState = tables.put(dbTable, newState);
    if (!newState.equals(oldState)) {
      save();
    }
  }

  private void save() {
    byte[] stateBytes = Bytes.toBytes(GSON.toJson(getState()));
    Failsafe.with(saveRetryPolicy).run(() -> stateStore.writeState(pipelineId, STATE_KEY, stateBytes));
  }

}
