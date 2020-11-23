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

import java.io.Closeable;
import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Fetches information about tables in a database. The registry is used when a user is configuring a delta pipeline.
 */
public interface TableRegistry extends Closeable {

  /**
   * @return list of readable tables
   * @throws IOException if the table information could not be read
   */
  TableList listTables() throws IOException;

  /**
   * Return details about a table.
   *
   * @param database name of the database that table resides in
   * @param table the table name
   * @param schema name of the schema that table resides in, it's only required for some DB to
   *               identify a table.
   * @return detail about the table
   * @throws TableNotFoundException if the specified table does not exist
   * @throws IOException if the table information could not be read
   */
  TableDetail describeTable(String database, String table, @Nullable String schema)
    throws TableNotFoundException, IOException;

  /**
   * Standardize raw column information into a standard schema that will be sent to the target.
   * If there are columns that are not supported by the source, an exception should be thrown.
   *
   * @param tableDetail raw table descriptor
   * @return standardized table descriptor
   */
  StandardizedTableDetail standardize(TableDetail tableDetail);
}
