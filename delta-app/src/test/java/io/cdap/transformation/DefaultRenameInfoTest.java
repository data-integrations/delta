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


import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@RunWith(JUnit4.class)
public class DefaultRenameInfoTest {

  @Test
  public void test() {

    Map<String, String> nameMapping = new HashMap<>();
    nameMapping.put("old", "new");
    DefaultRenameInfo renameInfo = new DefaultRenameInfo(nameMapping);
    Assert.assertEquals("new", renameInfo.getNewName("old"));
    Assert.assertEquals("unknown", renameInfo.getNewName("unknown"));
    Set<String> renamedColumns = new HashSet<>(nameMapping.keySet());
    Assert.assertEquals(renamedColumns, renameInfo.getRenamedColumns());

    //test immutability
    nameMapping.put("unkown", "unknown1");
    nameMapping.put("old", "new1");
    Assert.assertEquals("new", renameInfo.getNewName("old"));
    Assert.assertEquals("unknown", renameInfo.getNewName("unknown"));
    Assert.assertEquals(renamedColumns, renameInfo.getRenamedColumns());


  }

}
