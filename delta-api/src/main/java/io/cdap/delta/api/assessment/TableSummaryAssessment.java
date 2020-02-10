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
 * A summary of potential issues about a table.
 */
public class TableSummaryAssessment extends TableSummary {
  private final int numColumnsNotSupported;
  private final int numColumnsPartiallySupported;

  public TableSummaryAssessment(String database, String table, int numColumns, int numColumnsNotSupported,
                                int numColumnsPartiallySupported, @Nullable String schema) {
    super(database, table, numColumns, schema);
    this.numColumnsNotSupported = numColumnsNotSupported;
    this.numColumnsPartiallySupported = numColumnsPartiallySupported;
  }

  /**
   * @return number of columns that are not supported
   */
  public int getNumColumnsNotSupported() {
    return numColumnsNotSupported;
  }

  /**
   * Number of partially supported columns. A partially supported column is something that can be replicated, but with
   * some loss of data or change in structure. For example, precision in a date time may be lost,
   * or a database specific type might be translated into a string.
   *
   * @return number of columns that are partially supported.
   */
  public int getNumColumnsPartiallySupported() {
    return numColumnsPartiallySupported;
  }
}
