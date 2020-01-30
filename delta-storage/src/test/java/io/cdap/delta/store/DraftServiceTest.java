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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.test.SystemAppTestBase;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaTarget;
import io.cdap.delta.api.SourceColumn;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.api.assessment.ColumnAssessment;
import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.ColumnSuggestion;
import io.cdap.delta.api.assessment.ColumnSupport;
import io.cdap.delta.api.assessment.PipelineAssessment;
import io.cdap.delta.api.assessment.TableAssessment;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableSummary;
import io.cdap.delta.api.assessment.TableSummaryAssessment;
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
import java.util.Arrays;
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
                      new DeltaConfig(invalidSrc, target, Collections.emptyList()));
  }

  @Test
  public void testTableListAndDetail() throws Exception {
    DraftService service = new DraftService(getTransactionRunner());

    DraftId draftId = new DraftId(new Namespace("ns", 0L), "testTableListAndDetail");
    Stage src = new Stage("src",
                          new Plugin("mock", DeltaSource.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY));
    Stage target = new Stage("t",
                             new Plugin("oracle", DeltaTarget.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY));
    service.saveDraft(draftId, new DeltaConfig(src, target, Collections.emptyList()));

    TableList expectedList = new TableList(Collections.singletonList(new TableSummary("deebee", "taybull", 3)));
    List<ColumnDetail> columns = new ArrayList<>();
    columns.add(new ColumnDetail("id", JDBCType.INTEGER, false));
    columns.add(new ColumnDetail("name", JDBCType.VARCHAR, false));
    columns.add(new ColumnDetail("age", JDBCType.INTEGER, true));
    TableDetail expectedDetail = new TableDetail("deebee", "taybull", Collections.singletonList("id"), columns);

    MockTableRegistry mockTableRegistry = new MockTableRegistry(expectedList, expectedDetail, null);
    DeltaSource mockSource = new MockSource(mockTableRegistry, null);
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

  @Test
  public void testAssessTable() throws Exception {
    DraftService service = new DraftService(getTransactionRunner());
    DraftId draftId = new DraftId(new Namespace("ns", 0L), "testAssessTable");
    Stage src = new Stage("src",
                          new Plugin("mock", DeltaSource.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY));
    Stage target = new Stage("t",
                             new Plugin("oracle", DeltaTarget.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY));
    service.saveDraft(draftId, new DeltaConfig(src, target, Collections.emptyList()));

    TableList expectedList = new TableList(Collections.singletonList(new TableSummary("deebee", "taybull", 3)));
    List<ColumnDetail> columns = new ArrayList<>();
    columns.add(new ColumnDetail("id", JDBCType.INTEGER, false));
    columns.add(new ColumnDetail("name", JDBCType.VARCHAR, false));
    columns.add(new ColumnDetail("age", JDBCType.INTEGER, true));
    Schema schema = Schema.recordOf(
      "taybull",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("age", Schema.nullableOf(Schema.of(Schema.Type.INT))));
    TableDetail expectedDetail = new TableDetail("deebee", "taybull", Collections.singletonList("id"), columns);


    List<ColumnAssessment> columnAssessments = new ArrayList<>();
    columnAssessments.add(new ColumnAssessment("id", JDBCType.INTEGER.getName()));
    columnAssessments.add(
      new ColumnAssessment("name", JDBCType.VARCHAR.getName(), ColumnSupport.NO,
                           new ColumnSuggestion("msg", Collections.emptyList())));
    columnAssessments.add(new ColumnAssessment("age", JDBCType.INTEGER.getName()));

    TableAssessment expected = new TableAssessment(columnAssessments);
    MockTableAssessor mockAssessor = new MockTableAssessor<>(expected);
    MockTableRegistry mockTableRegistry = new MockTableRegistry(expectedList, expectedDetail, schema);
    DeltaSource mockSource = new MockSource(mockTableRegistry, mockAssessor);
    DeltaTarget mockTarget = new MockTarget(mockAssessor);
    Configurer mockConfigurer = new MockConfigurer(mockSource, mockTarget);

    TableAssessment actual = service.assessTable(draftId, mockConfigurer, "deebee", "taybull");
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testAssessPipeline() throws Exception {
    DraftService service = new DraftService(getTransactionRunner());
    DraftId draftId = new DraftId(new Namespace("ns", 0L), "testAssessPipeline");
    Stage src = new Stage("src",
                          new Plugin("mock", DeltaSource.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY));
    Stage target = new Stage("t",
                             new Plugin("oracle", DeltaTarget.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY));
    // configure the pipeline to read 2 out of the 3 columns from the table
    SourceTable sourceTable = new SourceTable("deebee", "taybull",
                                              Arrays.asList(new SourceColumn("id"), new SourceColumn("name")));
    service.saveDraft(draftId, new DeltaConfig(src, target, Collections.singletonList(sourceTable)));

    TableList expectedList = new TableList(Collections.singletonList(new TableSummary("deebee", "taybull", 3)));
    List<ColumnDetail> columns = new ArrayList<>();
    columns.add(new ColumnDetail("id", JDBCType.INTEGER, false));
    columns.add(new ColumnDetail("name", JDBCType.VARCHAR, false));
    columns.add(new ColumnDetail("age", JDBCType.INTEGER, true));
    Schema schema = Schema.recordOf(
      "taybull",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    TableDetail expectedDetail = new TableDetail("deebee", "taybull", Collections.singletonList("id"), columns);

    List<ColumnAssessment> columnAssessments = new ArrayList<>();
    columnAssessments.add(new ColumnAssessment("id", JDBCType.INTEGER.getName()));
    columnAssessments.add(
      new ColumnAssessment("name", JDBCType.VARCHAR.getName(), ColumnSupport.NO,
                           new ColumnSuggestion("msg", Collections.emptyList())));
    TableAssessment expectedTableAssessment = new TableAssessment(columnAssessments);

    MockTableAssessor mockAssessor = new MockTableAssessor(expectedTableAssessment);
    MockTableRegistry mockTableRegistry = new MockTableRegistry(expectedList, expectedDetail, schema);
    DeltaSource mockSource = new MockSource(mockTableRegistry, mockAssessor);
    DeltaTarget mockTarget = new MockTarget(mockAssessor);
    Configurer mockConfigurer = new MockConfigurer(mockSource, mockTarget);

    TableSummaryAssessment summaryAssessment = new TableSummaryAssessment("deebee", "taybull", 2, 1, 0);
    PipelineAssessment expected = new PipelineAssessment(Collections.singletonList(summaryAssessment),
                                                         Collections.emptyList(), Collections.emptyList());
    PipelineAssessment actual = service.assessPipeline(draftId, mockConfigurer);
    Assert.assertEquals(expected, actual);
  }
}
