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

import java.util.Objects;

/**
 * Represents a DML Operation.
 */
public class DMLOperation {

  /**
   * Type of DML operation.
   */
  public enum Type {
    INSERT,
    DELETE,
    UPDATE
  }

  private final String tableName;
  private final DMLOperation.Type type;
  private final long ingestTimestampMillis;
  private final int sizeInBytes;

  public DMLOperation(String tableName, DMLOperation.Type operationType, long ingestTimestampMillis, int sizeInBytes) {
    this.tableName = tableName;
    this.type = operationType;
    this.ingestTimestampMillis = ingestTimestampMillis;
    this.sizeInBytes = sizeInBytes;
  }

  public String getTableName() {
    return tableName;
  }

  public Type getType() {
    return type;
  }

  public long getIngestTimestampMillis() {
    return ingestTimestampMillis;
  }

  public int getSizeInBytes() {
    return sizeInBytes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DMLOperation that = (DMLOperation) o;
    return tableName.equals(that.tableName) &&
      type == that.type &&
      ingestTimestampMillis == that.ingestTimestampMillis &&
      sizeInBytes == that.sizeInBytes;
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, type, ingestTimestampMillis, sizeInBytes);
  }
}
