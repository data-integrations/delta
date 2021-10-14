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

package io.cdap.delta.test.mock;

import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.transformation.api.RowSchema;
import io.cdap.transformation.api.RowValue;
import io.cdap.transformation.api.Transformation;
import io.cdap.transformation.api.TransformationContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Mock Transformation
 */
@io.cdap.cdap.api.annotation.Plugin(type = Transformation.PLUGIN_TYPE)
@Name(MockTransformation.NAME)
public class MockTransformation implements Transformation {
  public static final String NAME = "mock";
  private final Map<String, Object> valuesMap;
  private final List<Schema.Field> fields;
  private final Map<String, String> renameMap;

  public MockTransformation(List<Schema.Field> fields, Map<String, String> renameMap, Map<String, Object> valuesMap) {
    this.valuesMap = valuesMap == null ? Collections.emptyMap() : valuesMap;
    this.fields = fields == null ? Collections.emptyList() : fields;
    this.renameMap = renameMap;
  }

  @Override
  public void initialize(TransformationContext context) throws Exception {
  }

  @Override
  public void transformValue(RowValue rowValue) throws Exception {
    for (Map.Entry<String, Object> entry : valuesMap.entrySet()) {
      rowValue.setColumnValue(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, String> entry : renameMap.entrySet()) {
      rowValue.renameColumn(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void transformSchema(RowSchema rowSchema) throws Exception {
    for (Schema.Field field : fields) {
      rowSchema.setField(field);
    }
    for (Map.Entry<String, String> entry : renameMap.entrySet()) {
      rowSchema.renameField(entry.getKey(), entry.getValue());
    }
  }
}
