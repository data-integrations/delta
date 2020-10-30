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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Defines what types of events should a reader should emit.
 */
public class EventReaderDefinition {
  private final Set<SourceTable> tables;
  private final Set<DMLOperation.Type> dmlBlacklist;
  private final Set<DDLOperation.Type> ddlBlacklist;

  public EventReaderDefinition(Set<SourceTable> tables, Set<DMLOperation.Type> dmlBlacklist,
                               Set<DDLOperation.Type> ddlBlacklist) {
    this.tables = tables;
    this.dmlBlacklist = Collections.unmodifiableSet(new HashSet<>(dmlBlacklist));
    this.ddlBlacklist = Collections.unmodifiableSet(new HashSet<>(ddlBlacklist));
  }

  /**
   * @return tables that should be read. If empty, it means all tables should be read.
   */
  public Set<SourceTable> getTables() {
    return tables;
  }

  /**
   * @return set of DDL operations that should always be ignored, regardless of which table they are related to.
   */
  public Set<DDLOperation.Type> getDdlBlacklist() {
    return ddlBlacklist;
  }

  /**
   * @return set of DML operations that should always be ignored, regardless of which table they are related to.
   */
  public Set<DMLOperation.Type> getDmlBlacklist() {
    return dmlBlacklist;
  }
}
