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

import static org.junit.Assert.assertEquals;

public class ColumnTransformationTest {

  @Test(expected = IllegalArgumentException.class)
  public void testNullColumnName() {
    new ColumnTransformation(null, "rename ss").validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyColumnName() {
    new ColumnTransformation("", "rename ss").validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullDirective() {
    new ColumnTransformation("name", null).validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyDirective() {
    new ColumnTransformation("name", "").validate();
  }

  @Test
  public void testGet() {
    ColumnTransformation columnTransformation = new ColumnTransformation("name", "rename name");
    assertEquals("name", columnTransformation.getColumnName());
    assertEquals("rename name", columnTransformation.getDirective());
  }
}
