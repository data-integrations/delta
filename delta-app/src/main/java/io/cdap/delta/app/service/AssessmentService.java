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

package io.cdap.delta.app.service;

import io.cdap.cdap.api.service.AbstractSystemService;
import io.cdap.delta.store.DBReplicationOffsetStore;
import io.cdap.delta.store.DBReplicationStateStore;
import io.cdap.delta.store.DraftStore;

/**
 * System service for storing drafts, offsets, state store data and performing assessments.
 */
public class AssessmentService extends AbstractSystemService {
  public static final String NAME = "assessor";

  @Override
  protected void configure() {
    setName(NAME);
    addHandler(new AssessmentHandler());
    addHandler(new OffsetStateHandler());
    createTable(DraftStore.TABLE_SPEC);
    createTable(DBReplicationStateStore.TABLE_SPEC);
    createTable(DBReplicationOffsetStore.TABLE_SPEC);
  }
}
