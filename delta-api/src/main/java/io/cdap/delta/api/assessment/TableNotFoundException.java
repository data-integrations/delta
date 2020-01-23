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

package io.cdap.delta.api.assessment;

/**
 * Thrown when a table is unexpectedly not found.
 */
public class TableNotFoundException extends Exception {

  public TableNotFoundException(String database, String table, String errorMessage) {
    super(String.format("Table '%s' in database '%s' was not found. %s", table, database, errorMessage));
  }

  public TableNotFoundException(String database, String table, String errorMessage, Throwable cause) {
    super(String.format("Table '%s' in database '%s' was not found. %s", table, database, errorMessage), cause);
  }
}
