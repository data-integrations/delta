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
   * and actually written out when {@link #commitOffset(Offset)} is called.
   *
   * @param op type of DML operation
   */
  void incrementCount(DMLOperation op);

  /**
   * Record that a DDL operation was processed. Metrics on the number of operations processed are stored in memory
   * and actually written out when {@link #commitOffset(Offset)} is called.
   *
   * @param op type of DDL operation
   */
  void incrementCount(DDLOperation op);

  /**
   * Commit changes up to the given offset. Once an offset is successfully committed, events up to that offset are
   * considered complete. When the program starts up, events from the last committed offset will be read. If the
   * program died before committing an offset, events may be replayed.
   *
   * Also writes out metrics on the number of DML and DDL operations that were applied since the last successful
   * commit, as recorded by calls to {@link #incrementCount(DDLOperation)} and {@link #incrementCount(DMLOperation)}.
   *
   * @param offset offset to commitOffset
   */
  void commitOffset(Offset offset) throws IOException;
}
