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

/**
 * Emits change events. Events should be emitted in the order that they occurred.
 *
 * The order that events are emitted is the order in which they will eventually be applied.
 */
public interface EventEmitter {

  /**
   * Emits a DDL event. DDL events should not be emitted within a transaction.
   * This call may block if the pipeline target is not keeping up with the source.
   *
   * @param event the DDL event
   * @throws InterruptedException if it was interrupted while blocked on emitting the event.
   *   The event is dropped in this scenario.
   */
  boolean emit(DDLEvent event) throws InterruptedException;

  /**
   * Emits a DML event within its own transaction.
   * This call may block if the pipeline target is not keeping up with the source.
   *
   * @param event the DML event
   * @throws InterruptedException if it was interrupted while blocked on emitting the event.
   *   The event is dropped in this scenario.
   */
  boolean emit(DMLEvent event) throws InterruptedException;
}
