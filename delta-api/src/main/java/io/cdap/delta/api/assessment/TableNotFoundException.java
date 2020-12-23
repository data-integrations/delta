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

import javax.annotation.Nullable;

/**
 * Thrown when a table is unexpectedly not found.
 */
public class TableNotFoundException extends Exception {

  public TableNotFoundException(String database, String table, String errorMessage) {
    this(database, null, table, errorMessage);
  }

  public TableNotFoundException(String database, @Nullable String schema, String table, String errorMessage) {
    super(buildErrorMessage(database, schema, table, errorMessage));
  }

  public TableNotFoundException(String database, String table, String errorMessage, Throwable cause) {
    this(database, null, table, errorMessage, cause);
  }

  public TableNotFoundException(String database, @Nullable String schema, String table, String errorMessage,
    Throwable cause) {
    super(buildErrorMessage(database, schema, table, errorMessage), cause);
  }

  private static String buildErrorMessage(String database, @Nullable String schema, String table,
    String errorMessage) {
    String schemaInfo = schema == null ? "" : String.format("and schema '%s' ", schema);
    return String.format("Table '%s' in database '%s' %swas not found. %s", table, database, schemaInfo, errorMessage);
  }
}
