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
import io.cdap.delta.proto.DraftRequest;
import io.cdap.delta.proto.FullColumnAssessment;
import io.cdap.delta.proto.Plugin;
import io.cdap.delta.proto.Stage;
import io.cdap.delta.proto.TableAssessmentResponse;
import io.cdap.delta.test.mock.MockConfigurer;
import io.cdap.delta.test.mock.MockTableAssessor;
import io.cdap.delta.test.mock.MockTableRegistry;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    DraftService service = new DraftService(getTransactionRunner(), NoOpPropertyEvaluator.INSTANCE);
    service.getDraft(new DraftId(new Namespace("ns", 0L), "testDraftNotFound"));
  }

  @Test
  public void testTableListAndDetail() throws Exception {
    DraftService service = new DraftService(getTransactionRunner(), NoOpPropertyEvaluator.INSTANCE);

    DraftId draftId = new DraftId(new Namespace("ns", 0L), "testTableListAndDetail");
    Stage src = new Stage("src",
                          new Plugin("mock", DeltaSource.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY));
    Stage target = new Stage("t",
                             new Plugin("oracle", DeltaTarget.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY));
    DeltaConfig deltaConfig = DeltaConfig.builder().setSource(src).setTarget(target).build();
    service.saveDraft(draftId, new DraftRequest("label", deltaConfig));

    TableList expectedList = new TableList(Collections.singletonList(new TableSummary("deebee", "taybull", 3, null)));
    List<ColumnDetail> columns = new ArrayList<>();
    columns.add(new ColumnDetail("id", JDBCType.INTEGER, false));
    columns.add(new ColumnDetail("name", JDBCType.VARCHAR, false));
    columns.add(new ColumnDetail("age", JDBCType.INTEGER, true));
    TableDetail expectedDetail = TableDetail.builder("deebee", "taybull", null)
      .setPrimaryKey(Collections.singletonList("id"))
      .setColumns(columns)
      .build();

    MockTableRegistry mockTableRegistry = new MockTableRegistry(expectedList, expectedDetail, null);
    DeltaSource mockSource = new MockSource(mockTableRegistry, null);
    Configurer mockConfigurer = new MockConfigurer(mockSource, null);
    Assert.assertEquals(expectedList, service.listDraftTables(draftId, mockConfigurer));
    Assert.assertEquals(expectedDetail, service.describeDraftTable(draftId, mockConfigurer, "deebee", null, "taybull"));
  }

  @Test(expected = DraftNotFoundException.class)
  public void testListTablesFromNonexistantDraft() throws Exception {
    DraftService service = new DraftService(getTransactionRunner(), NoOpPropertyEvaluator.INSTANCE);
    service.listDraftTables(new DraftId(new Namespace("ns", 0L), "testListTablesFromNonexistantDraft"),
                            new MockConfigurer(null, null));
  }

  @Test
  public void testAssessTable() throws Exception {
    DraftService service = new DraftService(getTransactionRunner(), NoOpPropertyEvaluator.INSTANCE);
    DraftId draftId = new DraftId(new Namespace("ns", 0L), "testAssessTable");
    DeltaConfig config = DeltaConfig.builder()
      .setSource(new Stage("src", new Plugin("mock", DeltaSource.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY)))
      .setTarget(new Stage("t", new Plugin("oracle", DeltaTarget.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY)))
      .setTables(Collections.singletonList(new SourceTable("deebee", "taybull")))
      .build();
    service.saveDraft(draftId, new DraftRequest("label", config));

    TableList expectedList = new TableList(Collections.singletonList(new TableSummary("deebee", "taybull", 3, null)));
    List<ColumnDetail> columns = new ArrayList<>();
    columns.add(new ColumnDetail("id", JDBCType.INTEGER, false));
    columns.add(new ColumnDetail("name", JDBCType.VARCHAR, false));
    columns.add(new ColumnDetail("age", JDBCType.INTEGER, true));
    Schema schema = Schema.recordOf(
      "taybull",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("age", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("skip", Schema.of(Schema.Type.BOOLEAN)));
    TableDetail expectedDetail = TableDetail.builder("deebee", "taybull", null)
      .setPrimaryKey(Collections.singletonList("id"))
      .setColumns(columns)
      .build();

    List<ColumnAssessment> columnAssessments = new ArrayList<>();
    columnAssessments.add(ColumnAssessment.builder("id", "int").setSourceColumn("id").build());
    columnAssessments.add(ColumnAssessment.builder("name", "varchar")
                            .setSourceColumn("name")
                            .setSupport(ColumnSupport.NO)
                            .setSuggestion(new ColumnSuggestion("msg", Collections.emptyList()))
                            .build());
    columnAssessments.add(ColumnAssessment.builder("age", "int").setSourceColumn("age").build());

    TableAssessment expectedAssessment = new TableAssessment(columnAssessments, Collections.emptyList());
    MockTableAssessor mockAssessor = new MockTableAssessor<>(expectedAssessment);
    MockTableRegistry mockTableRegistry = new MockTableRegistry(expectedList, expectedDetail, schema);
    DeltaSource mockSource = new MockSource(mockTableRegistry, mockAssessor);
    DeltaTarget mockTarget = new MockTarget(mockAssessor);
    Configurer mockConfigurer = new MockConfigurer(mockSource, mockTarget);

    List<FullColumnAssessment> fullColumns = columnAssessments.stream()
      .map(c -> new FullColumnAssessment(c.getSupport(), c.getName(), c.getType(), c.getName(), c.getType(),
                                         c.getSuggestion()))
      .collect(Collectors.toList());
    TableAssessmentResponse expected = new TableAssessmentResponse(fullColumns, Collections.emptyList());
    TableAssessmentResponse actual = service.assessTable(draftId, mockConfigurer, "deebee", null, "taybull");
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testAssessPipeline() throws Exception {
    DraftService service = new DraftService(getTransactionRunner(), NoOpPropertyEvaluator.INSTANCE);
    DraftId draftId = new DraftId(new Namespace("ns", 0L), "testAssessPipeline");
    // configure the pipeline to read 2 out of the 3 columns from the table
    SourceTable sourceTable = new SourceTable(
      "deebee", "taybull", new HashSet<>(Arrays.asList(new SourceColumn("id"), new SourceColumn("name"),
            new SourceColumn("suppressed", true))));
    DeltaConfig config = DeltaConfig.builder()
      .setSource(new Stage("src", new Plugin("mock", DeltaSource.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY)))
      .setTarget(new Stage("t", new Plugin("oracle", DeltaTarget.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY)))
      .setTables(Collections.singletonList(sourceTable))
      .build();
    service.saveDraft(draftId, new DraftRequest("label", config));

    TableList expectedList = new TableList(Collections.singletonList(new TableSummary("deebee", "taybull", 3, null)));
    List<ColumnDetail> columns = new ArrayList<>();
    columns.add(new ColumnDetail("id", JDBCType.INTEGER, false));
    columns.add(new ColumnDetail("name", JDBCType.VARCHAR, false));
    columns.add(new ColumnDetail("age", JDBCType.INTEGER, true));
    columns.add(new ColumnDetail("suppressed", JDBCType.INTEGER, true));
    Schema schema = Schema.recordOf(
      "taybull",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    TableDetail expectedDetail = TableDetail.builder("deebee", "taybull", null)
      .setPrimaryKey(Collections.singletonList("id"))
      .setColumns(columns)
      .build();

    List<ColumnAssessment> columnAssessments = new ArrayList<>();
    columnAssessments.add(ColumnAssessment.builder("id", "int").setSourceColumn("id").build());
    columnAssessments.add(ColumnAssessment.builder("name", "varchar")
                            .setSourceColumn("name")
                            .setSupport(ColumnSupport.NO)
                            .setSuggestion(new ColumnSuggestion("msg", Collections.emptyList()))
                            .build());
    columnAssessments.add(ColumnAssessment.builder("suppressed", "int")
                            .setSourceColumn("suppressed")
                            .setSupport(ColumnSupport.PARTIAL)
                            .setSuggestion(new ColumnSuggestion("msg", Collections.emptyList()))
                            .build());
    TableAssessment expectedTableAssessment = new TableAssessment(columnAssessments, Collections.emptyList());

    MockTableAssessor mockAssessor = new MockTableAssessor(expectedTableAssessment);
    MockTableRegistry mockTableRegistry = new MockTableRegistry(expectedList, expectedDetail, schema);
    DeltaSource mockSource = new MockSource(mockTableRegistry, mockAssessor);
    DeltaTarget mockTarget = new MockTarget(mockAssessor);
    Configurer mockConfigurer = new MockConfigurer(mockSource, mockTarget);

    TableSummaryAssessment summaryAssessment = new TableSummaryAssessment("deebee", "taybull", 3, 1, 0, null);
    PipelineAssessment expected = new PipelineAssessment(Collections.singletonList(summaryAssessment),
                                                         Collections.emptyList(), Collections.emptyList());
    PipelineAssessment actual = service.assessPipeline(draftId, mockConfigurer);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testMacroEvaluation() throws Exception {
    DraftId draftId = new DraftId(new Namespace("ns", 0L), "testAssessPipeline");

    TableList expectedList = new TableList(Collections.singletonList(new TableSummary("deebee", "taybull", 3, null)));
    List<ColumnDetail> columns = new ArrayList<>();
    columns.add(new ColumnDetail("id", JDBCType.INTEGER, false));
    columns.add(new ColumnDetail("name", JDBCType.VARCHAR, false));
    columns.add(new ColumnDetail("age", JDBCType.INTEGER, true));
    Schema schema = Schema.recordOf(
      "taybull",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    TableDetail expectedDetail = TableDetail.builder("deebee", "taybull", null)
      .setPrimaryKey(Collections.singletonList("id"))
      .setColumns(columns)
      .build();

    List<ColumnAssessment> columnAssessments = new ArrayList<>();
    columnAssessments.add(ColumnAssessment.builder("id", "int").setSourceColumn("id").build());
    columnAssessments.add(ColumnAssessment.builder("name", "varchar")
                            .setSourceColumn("name")
                            .setSupport(ColumnSupport.NO)
                            .setSuggestion(new ColumnSuggestion("msg", Collections.emptyList()))
                            .build());
    TableAssessment expectedTableAssessment = new TableAssessment(columnAssessments, Collections.emptyList());

    Map<String, String> pluginProps = PropertyBasedMockConfigurer.createMap(expectedList, expectedDetail,
                                                                            schema, expectedTableAssessment,
                                                                            expectedTableAssessment);
    PropertyEvaluator propertyEvaluator = new MockPropertyEvaluator(pluginProps);
    DeltaConfig config = DeltaConfig.builder()
      .setSource(new Stage("src", new Plugin("mock", DeltaSource.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY)))
      .setTarget(new Stage("tgt", new Plugin("mock", DeltaTarget.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY)))
      .setTables(Collections.singletonList(new SourceTable("deebee", "taybull")))
      .build();

    DraftService service = new DraftService(getTransactionRunner(), propertyEvaluator);
    service.saveDraft(draftId, new DraftRequest("label", config));

    List<FullColumnAssessment> fullColumns = columnAssessments.stream()
      .map(c -> new FullColumnAssessment(c.getSupport(), c.getName(), c.getType(), c.getName(), c.getType(),
                                         c.getSuggestion()))
      .collect(Collectors.toList());
    TableAssessmentResponse expectedAssessment = new TableAssessmentResponse(fullColumns, Collections.emptyList());
    Configurer configurer = new PropertyBasedMockConfigurer();
    Assert.assertEquals(expectedList, service.listDraftTables(draftId, configurer));
    Assert.assertEquals(expectedDetail, service.describeDraftTable(draftId, configurer, "deebee", null, "taybull"));
    Assert.assertEquals(expectedAssessment, service.assessTable(draftId, configurer, "deebee", null, "taybull"));
  }
}
