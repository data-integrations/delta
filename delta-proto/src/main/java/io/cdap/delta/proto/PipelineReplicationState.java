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

package io.cdap.delta.proto;

import io.cdap.delta.api.ReplicationError;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * State of the overall replication pipeline.
 */
public class PipelineReplicationState {
  public static final PipelineReplicationState EMPTY =
    new PipelineReplicationState(PipelineState.OK, Collections.emptySet(), null);
  private final Set<TableReplicationState> tables;
  private final PipelineState sourceState;
  private final ReplicationError sourceError;

  public PipelineReplicationState(PipelineState sourceState, Set<TableReplicationState> tables,
                                  @Nullable ReplicationError error) {
    this.sourceState = sourceState;
    this.tables = tables;
    this.sourceError = error;
  }

  public PipelineState getSourceState() {
    return sourceState;
  }

  public Set<TableReplicationState> getTables() {
    return tables;
  }

  @Nullable
  public ReplicationError getSourceError() {
    return sourceError;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PipelineReplicationState that = (PipelineReplicationState) o;
    return sourceState == that.sourceState &&
      Objects.equals(tables, that.tables) &&
      Objects.equals(sourceError, that.sourceError);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sourceState, tables, sourceError);
  }
}
