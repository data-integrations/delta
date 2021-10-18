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

package io.cdap.transformation;

import io.cdap.transformation.api.NotFoundException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class DefaultMutableRowValueTest {

  @Test(expected = IllegalArgumentException.class)
  public void testNullValueMap() {
    new DefaultRowValue(null);
  }

  @Test(expected = NotFoundException.class)
  public void testGetNonexisting() throws Exception {
    new DefaultRowValue().getColumnValue("non");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetNullColumnName() {
    new DefaultRowValue().setColumnValue(null, "value");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRenameNonexisting() {
    new DefaultRowValue().renameColumn("non", "new");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRenameToExisting() {
    DefaultRowValue defaultRowValue = new DefaultRowValue();
    defaultRowValue.setColumnValue("col1", "value1");
    defaultRowValue.setColumnValue("col2", "value2");
    defaultRowValue.renameColumn("col1", "col2");
  }

  @Test
  public void testGetSetRename() throws Exception {

    Map<String, Object> valuesMap = new HashMap<>();
    valuesMap.put("col1", "value1");
    valuesMap.put("col2", true);
    DefaultRowValue rowValue = new DefaultRowValue(valuesMap);
    //get
    Assert.assertEquals("value1", rowValue.getColumnValue("col1"));
    Assert.assertEquals(Boolean.TRUE, rowValue.getColumnValue("col2"));

    //set
    rowValue.setColumnValue("col1", "value2");
    Assert.assertEquals("value2", rowValue.getColumnValue("col1"));

    //rename
    rowValue.renameColumn("col1", "col3");
    Assert.assertEquals("value2", rowValue.getColumnValue("col3"));

  }

}
