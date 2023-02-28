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
import java.util.Collections;

/**
 * Context for a CDC Source.
 */
public interface DeltaSourceContext extends DeltaRuntimeContext, FailureNotifier {

  /**
   * Record that there are currently errors reading change events.
   *
   * @param error information about the error
   * @throws IOException if there was an error persisting the change in state. This can only be thrown if there was
   *   a change in state.
   */
  void setError(ReplicationError error) throws IOException;

  /**
   * Record that there are no errors reading change events.
   *
   * @throws IOException if there was an error persisting the change in state. This can only be thrown if there was
   *   a change in state.
   */
  void setOK() throws IOException;

  /**
   * @return the offset that is committed back by the Target Plugin after a successful load operation by target
   *
   * This is intended to be used by source side to commit the true offset back to the Replication server
   *
   * For the scenario when no offset information is found : This method returns an Empty Offset object (i.e empty Map )
   *  Meaning the source has the handle the case of empty offset .
   *  Example : if (offset.get().isEmpty()) { Do some handling }
   */
  default Offset getCommittedOffset() throws IOException {
    return new Offset(Collections.emptyMap());
  }

  /**
   * Record that a DML event was generated by the source plugin. Metrics on the number of operations processed are
   * stored in memory and actually written out at configured interval
   *
   * @param op a DML operation
   */
  void incrementPublishCount(DMLOperation op);

  /**
   * Record that a DDL operation was generated by the source plugin. Metrics on the number of operations processed are
   * stored in memory and actually written out at configured interval.
   *
   * @param op a DDL operation
   */
  void incrementPublishCount(DDLOperation op);
}
