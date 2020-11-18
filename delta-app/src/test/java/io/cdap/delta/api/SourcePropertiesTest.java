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

import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class SourcePropertiesTest {
  @Test
  public void testSerDe() throws Exception {
    SourceProperties expected = new SourceProperties.Builder()
      .setOrdering(SourceProperties.Ordering.UN_ORDERED)
      .setRowIdKey(new RowIdKey(Arrays.asList("a", "b"))).build();
    Gson gson = new Gson();
    String s = gson.toJson(expected);
    SourceProperties another = gson.fromJson(s, SourceProperties.class);
    Assert.assertEquals(expected, another);
  }

  @Test
  public void testValidate() throws Exception {
    SourceProperties properties;
    try {
      properties = new SourceProperties.Builder()
        .setOrdering(SourceProperties.Ordering.UN_ORDERED).build();
      Assert.fail("Creating source properties should have failed since row id key is not provided but source is " +
                    "configured to generate unordered events.");
    } catch (IllegalArgumentException ignored) {
    }

    try {
      properties = new SourceProperties.Builder()
        .setRowIdKey(new RowIdKey(Arrays.asList("a", "b"))).build();
      Assert.fail("Creating source properties should have failed since row id key is provided but source is " +
                    "configured to generate ordered events.");
    } catch (IllegalArgumentException ignored) {
    }
  }
}
