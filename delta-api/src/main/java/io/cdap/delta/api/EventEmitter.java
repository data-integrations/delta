/*
 * Copyright © 2019 Cask Data, Inc.
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
   *
   * @param event the DDL event
   */
  void emit(DDLEvent event);

  /**
   * Emits a DML event within its own transaction.
   *
   * @param event the DML event
   */
  void emit(DMLEvent event);
}
