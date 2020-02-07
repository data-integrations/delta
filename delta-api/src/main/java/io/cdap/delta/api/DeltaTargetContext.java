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

/**
 * Context for a CDC Target.
 */
public interface DeltaTargetContext extends DeltaRuntimeContext {

  /**
   * Record that a DML operation was processed. Metrics on the number of operations processed are stored in memory
   * and actually written out when {@link #commitOffset(Offset, long)} is called.
   *
   * @param op type of DML operation
   */
  void incrementCount(DMLOperation op);

  /**
   * Record that a DDL operation was processed. Metrics on the number of operations processed are stored in memory
   * and actually written out when {@link #commitOffset(Offset, long)} is called.
   *
   * @param op type of DDL operation
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
   * commit, as recorded by calls to {@link #incrementCount(DDLOperation)} and {@link #incrementCount(DMLOperation)}.
   *
   * @param offset offset to commitOffset
   * @param sequenceNumber the sequence number for the offset
   */
  void commitOffset(Offset offset, long sequenceNumber) throws IOException;

  /**
   * Record that there are currently errors applying events to a specified table
   *
   * @param database database that the table is in
   * @param table table that is having errors
   * @param error information about the error
   */
  void setTableError(String database, String table, ReplicationError error);

  /**
   * Record that a table is being replicated
   *
   * @param database database that the table is in
   * @param table table name
   */
  void setTableReplicating(String database, String table);

  /**
   * Record that the initial snapshot for the table is being applied
   *
   * @param database database that the table is in
   * @param table table name
   */
  void setTableSnapshotting(String database, String table);

  /**
   * Record that the table was dropped and its state can be removed
   *
   * @param database database that the table is in
   * @param table table name
   */
  void dropTableState(String database, String table);
}
