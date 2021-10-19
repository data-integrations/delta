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

import java.util.HashMap;
import java.util.Map;

/**
 * Default implementation of {@link ColumnRenameInfo}
 */
public class DefaultRenameInfo implements ColumnRenameInfo {
  private final Map<String, String> originalToNewNames;

  public DefaultRenameInfo(Map<String, String> originalToNewNames) {
    this.originalToNewNames = new HashMap<>(originalToNewNames);
  }

  @Override
  public String getNewName(String originalName) {
    String newName = originalToNewNames.get(originalName);
    return newName == null ? originalName : newName;
  }
}
