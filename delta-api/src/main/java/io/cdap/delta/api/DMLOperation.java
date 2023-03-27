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
import java.util.StringJoiner;
import javax.annotation.Nullable;

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
    UPDATE;
  }

  private final String databaseName;
  private final String schemaName;
  private final String tableName;
  private final DMLOperation.Type type;
  private final long ingestTimestampMillis;
  private final int sizeInBytes;

  public DMLOperation(String databaseName, @Nullable String schemaName, String tableName,
    DMLOperation.Type operationType, long ingestTimestampMillis, int sizeInBytes) {
    this.databaseName = databaseName;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.type = operationType;
    this.ingestTimestampMillis = ingestTimestampMillis;
    this.sizeInBytes = sizeInBytes;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getSchemaName() {
    return schemaName;
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
    return Objects.equals(databaseName, that.databaseName) && Objects.equals(schemaName, that.schemaName) &&
      Objects.equals(tableName, that.tableName) && type == that.type &&
      ingestTimestampMillis == that.ingestTimestampMillis && sizeInBytes == that.sizeInBytes;
  }

  @Override
  public String toString() {
    StringJoiner stringJoiner = new StringJoiner(", ", "{", "}");
    stringJoiner.add("databaseName=" + databaseName);
    stringJoiner.add("type=" + type);
    if (schemaName != null) {
      stringJoiner.add("schemaName=" + schemaName);
    }
    stringJoiner.add("tableName=" + tableName);
    stringJoiner.add("ingestTimestampMillis=" + ingestTimestampMillis);
    stringJoiner.add("sizeInBytes=" + sizeInBytes);
    return stringJoiner.toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(databaseName, schemaName, tableName, type, ingestTimestampMillis, sizeInBytes);
  }
}
