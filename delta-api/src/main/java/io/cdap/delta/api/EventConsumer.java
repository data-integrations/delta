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
 * Consumes events emitted by a source and applies them to the target system.
 */
public interface EventConsumer {

  /**
   * Start the event consumer. This will be called before any other method is called.
   */
  void start();

  /**
   * Stop the event consumer.
   *
   * @throws InterruptedException if the consumer was interrupted while stopping
   */
  default void stop() throws InterruptedException {
    // no-op
  }

  /**
   * Apply a DDL event, such as creating a table. This method must be idempotent. For example, if the event is a table
   * creation and the table already exists, this method should not fail due to an attempt to create a table that
   * already exists.
   *
   * Idempotency is required because the event can be applied multiple times in failure scenarios.
   * During normal operation, an event will be applied exactly once.
   * In failure scenarios the event will be applied at least once.
   *
   * If this method throws a DeltaFailureException, the pipeline will fail immediately.
   * If any other type of Exception is thrown, pipeline offset and sequence number will be reset to the last
   * commit, and events will be re-applied. If exceptions are consecutively thrown for longer than the
   * configured pipeline timeout, retries will be abandoned and the pipeline will be fail.
   *
   * @param event ddl event to apply
   * @throws DeltaFailureException if there was an error and the pipeline should immediately be failed
   * @throws Exception if there was an error applying the change, but it might succeed in the future
   */
  void applyDDL(Sequenced<DDLEvent> event) throws Exception;

  /**
   * Apply a DML event. This method must be idempotent. For example, if there is an insert and the row already exists,
   * this method should not fail due to an attempt to insert a row that already exists, and it should not write
   * duplicate data.
   *
   * Idempotency is required because the batch of events can be applied multiple times in failure scenarios.
   * During normal operation, each batch will be applied exactly once.
   * In failure scenarios the batch will be applied at least once.
   *
   * If this method throws a DeltaFailureException, the pipeline will fail immediately.
   * If any other type of Exception is thrown, pipeline offset and sequence number will be reset to the last
   * commit, and events will be re-applied. If exceptions are consecutively thrown for longer than the
   * configured pipeline timeout, retries will be abandoned and the pipeline will be fail.
   *
   * @param event DML event to apply
   * @throws DeltaFailureException if there was an error and the pipeline should immediately be failed
   * @throws Exception if there was an error applying the change, but it might succeed in the future
   */
  void applyDML(Sequenced<DMLEvent> event) throws Exception;

}
