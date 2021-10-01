/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.transformation.api;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

/**
 * Interface for Transformation plugin
 */
public interface Transformation {
  String PLUGIN_TYPE = "transformation";

  /**
   * Initialize the Transformation plugin.
   *
   * @param context the transformation context
   * @throws Exception
   */
  void initialize(TransformationContext context) throws Exception;


  /**
   * Apply transformation on the value of a row. This method will be invoked when the value a row needs to be
   * transformed. e.g. When a {@link StructuredRecord} needs to be transformed at runtime.
   * This interface probably will change in the future to support multiple rows, e.g. flatten one row to multiple rows.
   * @param rowValue the mutable row value
   */
  void transformValue(RowValue rowValue) throws Exception;

  /**
   * Apply transformation on the schema of a row. This method will be invoked when the schema of a row needs to be.
   * For example :
   * 1. When the output {@link Schema} of the impacted table needs to be calculated at design time.
   * 2. when a {@link StructuredRecord} needs to be transformed at runtime, the schema {@link StructuredRecord} needs to
   * be transformed.
   *
   * @param rowSchema the mutable row schema
   */
  void transformSchema(RowSchema rowSchema) throws Exception;

}
