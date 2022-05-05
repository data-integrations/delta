/*
 * Copyright © 2022 Cask Data, Inc.
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

import java.util.Objects;

/**
 * Request body to assessTable on the fly (without saving a draft)
 */
public class TableAssessmentRequest {
  private final DBTable dBTable;
  private final DeltaConfig deltaConfig;

  public TableAssessmentRequest(DBTable dBTable, DeltaConfig config) {
    this.dBTable = dBTable;
    this.deltaConfig = config;
  }

  public DBTable getDBTable() {
    return dBTable;
  }

  public DeltaConfig getDeltaConfig() {
    return deltaConfig;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TableAssessmentRequest)) {
      return false;
    }
    TableAssessmentRequest that = (TableAssessmentRequest) o;
    return Objects.equals(dBTable, that.dBTable) && Objects.equals(deltaConfig, that.deltaConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dBTable, deltaConfig);
  }
}
