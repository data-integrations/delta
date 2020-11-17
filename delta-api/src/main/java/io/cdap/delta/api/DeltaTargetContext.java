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

package io.cdap.delta.api;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Context for a CDC Target.
 */
public interface DeltaTargetContext extends DeltaRuntimeContext {

  /**
   * Record that a DML operation was processed. Metrics on the number of operations processed are stored in memory
   * and actually written out when {@link #commitOffset(Offset, long)} is called.
   *
   * @param op a DML operation
   */
  void incrementCount(DMLOperation op);

  /**
   * Record that a DDL operation was processed. Metrics on the number of operations processed are stored in memory
   * and actually written out when {@link #commitOffset(Offset, long)} is called.
   *
   * @param op a DDL operation
   */
  void incrementCount(DDLOperation op);

  /**
   * Commit changes up to the given offset. Once an offset is successfully committed, events up to that offset are
   * considered complete. When the program starts up, events from the last committed offset will be read. If the
   * program died before committing an offset, events may be replayed. The sequence number given here must match
   * the sequence number of the event with the given offset in order to ensure that events with a particular offset
   * always have the same corresponding sequence number.
   *
   * Also writes out metrics on the number of DML and DDL operations that were applied since the last successful
   * commit, as recorded by calls to {@link #incrementCount(DDLOperation)} and
   * {@link #incrementCount(DMLOperation)}.
   *
   * @param offset offset to commitOffset
   * @param sequenceNumber the sequence number for the offset
   * @throws IOException if there was an error persisting the new offset
   */
  void commitOffset(Offset offset, long sequenceNumber) throws IOException;

  /**
   * Record that there are currently errors applying events to a specified table.
   *
   * @param database database that the table is in
   * @param table table that is having errors
   * @param error information about the error
   * @throws IOException if there was an error persisting the change in state. This can only be thrown if there was
   *   a change in state.
   */
  void setTableError(String database, String table, ReplicationError error) throws IOException;

  /**
   * Record that a table is being replicated. This should be called after the initial table snapshot has been applied,
   * and if there was a recovery from an error.
   *
   * @param database database that the table is in
   * @param table table name
   * @throws IOException if there was an error persisting the change in state. This can only be thrown if there was
   *   a change in state.
   */
  void setTableReplicating(String database, String table) throws IOException;

  /**
   * Record that the initial snapshot for the table is being applied
   *
   * @param database database that the table is in
   * @param table table name
   * @throws IOException if there was an error persisting the change in state. This can only be thrown if there was
   *   a change in state.
   */
  void setTableSnapshotting(String database, String table) throws IOException;

  /**
   * Record that the table was dropped and its state can be removed
   *
   * @param database database that the table is in
   * @param table table name
   * @throws IOException if there was an error persisting the change in state. This can only be thrown if there was
   *   a change in state.
   */
  void dropTableState(String database, String table) throws IOException;

  /**
   * @return the properties about source in the replicator pipeline which will be useful to targets.
   * If no such properties are provided by source {@code null} is returned
   */
  @Nullable
  SourceProperties getSourceProperties();
}
