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

package io.cdap.delta.store;

import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.test.SystemAppTestBase;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaTarget;
import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableSummary;
import io.cdap.delta.proto.Artifact;
import io.cdap.delta.proto.DeltaConfig;
import io.cdap.delta.proto.Plugin;
import io.cdap.delta.proto.Stage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tests for {@link DraftService}.
 */
public class DraftServiceTest extends SystemAppTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  @Before
  public void setupTest() throws Exception {
    getStructuredTableAdmin().create(DraftStore.TABLE_SPEC);
  }

  @After
  public void cleanupTest() throws Exception {
    getStructuredTableAdmin().drop(DraftStore.TABLE_SPEC.getTableId());
  }

  @Test(expected = DraftNotFoundException.class)
  public void testDraftNotFound() {
    DraftService service = new DraftService(getTransactionRunner());
    service.getDraft(new DraftId(new Namespace("ns", 0L), "testDraftNotFound"));
  }

  @Test(expected = InvalidDraftException.class)
  public void testSaveInvalidDraftFails() {
    DraftService service = new DraftService(getTransactionRunner());
    Stage invalidSrc = new Stage("src",
                                 new Plugin(null, DeltaSource.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY));
    Stage target = new Stage("t",
                             new Plugin("oracle", DeltaTarget.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY));
    service.saveDraft(new DraftId(new Namespace("ns", 0L), "testSaveInvalidDraftFails"),
                      new DeltaConfig(invalidSrc, target));
  }

  @Test
  public void testTableListAndDetail() throws Exception {
    DraftService service = new DraftService(getTransactionRunner());

    DraftId draftId = new DraftId(new Namespace("ns", 0L), "testTableListAndDetail");
    Stage src = new Stage("src",
                          new Plugin("mock", DeltaSource.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY));
    Stage target = new Stage("t",
                             new Plugin("oracle", DeltaTarget.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY));
    service.saveDraft(draftId, new DeltaConfig(src, target));

    TableList expectedList = new TableList(Collections.singletonList(new TableSummary("deebee", "taybull", 3)));
    List<ColumnDetail> columns = new ArrayList<>();
    columns.add(new ColumnDetail("id", JDBCType.INTEGER, false));
    columns.add(new ColumnDetail("name", JDBCType.VARCHAR, false));
    columns.add(new ColumnDetail("age", JDBCType.INTEGER, true));
    TableDetail expectedDetail = new TableDetail("deebee", "taybull", Collections.singletonList("id"), columns);
    DeltaSource mockSource = new MockSource(expectedList, expectedDetail);
    Configurer mockConfigurer = new MockConfigurer(mockSource, null);
    Assert.assertEquals(expectedList, service.listDraftTables(draftId, mockConfigurer));
    Assert.assertEquals(expectedDetail, service.describeDraftTable(draftId, mockConfigurer, "deebee", "taybull"));
  }

  @Test(expected = DraftNotFoundException.class)
  public void testListTablesFromNonexistantDraft() throws IOException {
    DraftService service = new DraftService(getTransactionRunner());
    service.listDraftTables(new DraftId(new Namespace("ns", 0L), "testListTablesFromNonexistantDraft"),
                            new MockConfigurer(null, null));
  }

}
