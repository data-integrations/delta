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

package io.cdap.delta.proto;


import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TableTransformationTest {

  @Test(expected = IllegalArgumentException.class)
  public void testNullTableName() {
    new TableTransformation(null, Arrays.asList(new ColumnTransformation("name", "rename"))).validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyTableName() {
    new TableTransformation("", Arrays.asList(new ColumnTransformation("name", "rename"))).validate();
  }

  @Test
  public void testGet() {
    List<ColumnTransformation> columnTransformations = Arrays.asList(new ColumnTransformation("name", "rename"));
    TableTransformation tableTransformation =  new TableTransformation("table", columnTransformations);
    assertEquals("table", tableTransformation.getTableName());
    assertEquals(columnTransformations, tableTransformation.getColumnTransformations());
  }
}
