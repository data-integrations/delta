/*
 * Copyright © 2020-2022 Cask Data, Inc.
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
import io.cdap.delta.api.assessment.StandardizedTableDetail;
import io.cdap.delta.api.assessment.TableAssessment;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableSummary;
import io.cdap.delta.api.assessment.TableSummaryAssessment;
import io.cdap.delta.macros.ConfigMacroEvaluator;
import io.cdap.delta.macros.PropertyEvaluator;
import io.cdap.delta.proto.Artifact;
import io.cdap.delta.proto.ColumnTransformation;
import io.cdap.delta.proto.DBTable;
import io.cdap.delta.proto.DeltaConfig;
import io.cdap.delta.proto.DraftRequest;
import io.cdap.delta.proto.FullColumnAssessment;
import io.cdap.delta.proto.Plugin;
import io.cdap.delta.proto.Stage;
import io.cdap.delta.proto.TableAssessmentResponse;
import io.cdap.delta.proto.TableTransformation;
import io.cdap.delta.test.mock.MockConfigurer;
import io.cdap.delta.test.mock.MockTableAssessor;
import io.cdap.delta.test.mock.MockTableRegistry;
import io.cdap.delta.test.mock.MockTransformation;
import io.cdap.transformation.api.Transformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Tests for {@link DraftService}.
 */
public class DraftServiceTest extends SystemAppTestBase {

  private static final ConfigMacroEvaluator MACRO_EVALUATOR = new ConfigMacroEvaluator(NoOpPropertyEvaluator.INSTANCE);

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
    DraftService service = new DraftService(getTransactionRunner(), MACRO_EVALUATOR);
    service.getDraft(new DraftId(new Namespace("ns", 0L), "testDraftNotFound"));
  }

  @Test
  public void testTableListAndDetail() throws Exception {
    DraftService service = new DraftService(getTransactionRunner(), MACRO_EVALUATOR);

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
    DBTable dbTable = new DBTable("deebee", null, "taybull");
    Assert.assertEquals(expectedDetail, service.describeDraftTable(draftId, mockConfigurer, dbTable));
  }

  @Test(expected = DraftNotFoundException.class)
  public void testListTablesFromNonexistantDraft() throws Exception {
    DraftService service = new DraftService(getTransactionRunner(), MACRO_EVALUATOR);
    service.listDraftTables(new DraftId(new Namespace("ns", 0L), "testListTablesFromNonexistantDraft"),
                            new MockConfigurer(null, null));
  }

  @Test
  public void testAssessTable() throws Exception {
    DraftService service = new DraftService(getTransactionRunner(), MACRO_EVALUATOR);
    Namespace namespace = new Namespace("ns", 0L);
    DraftId draftId = new DraftId(namespace, "testAssessTable");
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
    columns.add(new ColumnDetail("last-name", JDBCType.INTEGER, true));
    columns.add(new ColumnDetail("age", JDBCType.INTEGER, true));
    Schema schema = Schema.recordOf(
      "taybull",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("last-name", Schema.of(Schema.Type.STRING)),
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
    columnAssessments.add(ColumnAssessment.builder("last-name", "varchar")
                            .setSourceColumn("last-name")
                            .build());
    columnAssessments.add(ColumnAssessment.builder("age", "int").setSourceColumn("age").build());
    TableAssessment expectedSourceAssessment = new TableAssessment(columnAssessments, Collections.emptyList());
    MockTableAssessor mockSourceAssessor = new MockTableAssessor<>(expectedSourceAssessment);

    ColumnAssessment targetLastNameAssessment = ColumnAssessment.builder("last_name", "varchar")
      .setSourceColumn("last-name")
      .build();
    List<ColumnAssessment> targetAssessments =
      columnAssessments.stream()
        .map(col -> col.getName().equals("last-name") ? targetLastNameAssessment : col)
        .collect(Collectors.toList());
    TableAssessment expectedTargetAssessment = new TableAssessment(targetAssessments, Collections.emptyList());
    MockTableAssessor mockTargetAssessor = new MockTableAssessor<>(expectedTargetAssessment);

    MockTableRegistry mockTableRegistry = new MockTableRegistry(expectedList, expectedDetail, schema);
    DeltaSource mockSource = new MockSource(mockTableRegistry, mockSourceAssessor);
    DeltaTarget mockTarget = new MockTarget(mockTargetAssessor);
    Configurer mockConfigurer = new MockConfigurer(mockSource, mockTarget);

    List<FullColumnAssessment> fullColumns = new ArrayList<>();
    for (int i = 0; i < columnAssessments.size(); i++) {
      ColumnAssessment src = columnAssessments.get(i);
      ColumnAssessment target = targetAssessments.get(i);
      fullColumns.add(new FullColumnAssessment(src.getSupport(), src.getName(), src.getType(), target.getName(),
        target.getType(), src.getSuggestion()));
    }

    TableAssessmentResponse expected = new TableAssessmentResponse(fullColumns, Collections.emptyList(),
                                                                   Collections.emptyList());
    DBTable dbTable = new DBTable("deebee", "taybull");
    // assessTable using DRAFT
    TableAssessmentResponse actualWithDraft = service.assessTable(namespace, config, mockConfigurer, dbTable);
    Assert.assertEquals(expected, actualWithDraft);
    // assessTable on the fly - with delta config
    TableAssessmentResponse actualOntheFly = service.assessTable(namespace, config,
                                                                 mockConfigurer, dbTable);
    Assert.assertEquals(expected, actualOntheFly);
  }

  @Test
  public void testAssessPipeline() throws Exception {
    DraftService service = new DraftService(getTransactionRunner(), MACRO_EVALUATOR);
    Namespace namespace = new Namespace("ns", 0L);
    DraftId draftId = new DraftId(namespace, "testAssessPipeline");
    // configure the pipeline to read 2 out of the 3 columns from the table
    SourceTable sourceTable = new SourceTable(
      "deebee", "taybull", new HashSet<>(Arrays.asList(new SourceColumn("id"), new SourceColumn("name"),
                                                       new SourceColumn("suppressed", true))));
    DeltaConfig config = DeltaConfig.builder()
                           .setSource(new Stage("src", new Plugin("mock", DeltaSource.PLUGIN_TYPE,
                                                                  Collections.emptyMap(), Artifact.EMPTY)))
                           .setTarget(new Stage("t", new Plugin("oracle", DeltaTarget.PLUGIN_TYPE,
                                                                Collections.emptyMap(), Artifact.EMPTY)))
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
                                                         Collections.emptyList(), Collections.emptyList(),
                                                         Collections.emptyList());
    PipelineAssessment actual = service.assessPipeline(namespace, config, mockConfigurer);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testAssessPipelineWithoutSaving() throws Exception {
    DraftService service = new DraftService(getTransactionRunner(), MACRO_EVALUATOR);
    DraftId draftId = new DraftId(new Namespace("ns", 0L), "testAssessPipelineInterim");
    // configure the pipeline to read 2 out of the 3 columns from the table
    SourceTable sourceTable = new SourceTable(
      "deebee", "taybull", new HashSet<>(Arrays.asList(new SourceColumn("id"), new SourceColumn("name"),
                                                       new SourceColumn("suppressed", true))));
    DeltaConfig config = DeltaConfig.builder()
      .setSource(new Stage("src", new Plugin("mock", DeltaSource.PLUGIN_TYPE,
                                             Collections.emptyMap(), Artifact.EMPTY)))
      .setTarget(new Stage("t", new Plugin("oracle", DeltaTarget.PLUGIN_TYPE,
                                           Collections.emptyMap(), Artifact.EMPTY)))
      .setTables(Collections.singletonList(sourceTable))
      .build();

    Draft draft = new Draft("testAssessPipelineInterim", "label", config, 0, 0);

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
                                                         Collections.emptyList(), Collections.emptyList(),
                                                         Collections.emptyList());
    PipelineAssessment actual = service.assessPipeline(draftId.getNamespace(), draft.getConfig(), mockConfigurer);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testAssessPipelineWithTransformations() throws Exception {
    DraftService service = new DraftService(getTransactionRunner(), MACRO_EVALUATOR);
    Namespace namespace = new Namespace("ns", 0L);
    DraftId draftId = new DraftId(namespace, "testAssessPipeline");
    // configure the pipeline to read 2 out of the 3 columns from the table
    SourceTable sourceTable = new SourceTable(
      "deebee", "taybull", new HashSet<>(Arrays.asList(new SourceColumn("id"), new SourceColumn("name"),
            new SourceColumn("suppressed", true))));

    TableTransformation tableTransformation = new TableTransformation("taybull",
      Collections.singletonList(new ColumnTransformation("id", MockTransformation.NAME)));
    DeltaConfig config = DeltaConfig.builder()
      .setSource(new Stage("src", new Plugin("mock", DeltaSource.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY)))
      .setTarget(new Stage("t", new Plugin("oracle", DeltaTarget.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY)))
      .setTables(Collections.singletonList(sourceTable))
      .setTableTransformations(Collections.singletonList(tableTransformation))
      .build();
    service.saveDraft(draftId, new DraftRequest("label", config));

    TableList expectedList = new TableList(Collections.singletonList(new TableSummary("deebee", "taybull", 3, null)));
    List<ColumnDetail> columns = new ArrayList<>();
    columns.add(new ColumnDetail("id", JDBCType.INTEGER, false));
    columns.add(new ColumnDetail("name", JDBCType.VARCHAR, false));
    columns.add(new ColumnDetail("age", JDBCType.INTEGER, true));
    columns.add(new ColumnDetail("suppressed", JDBCType.INTEGER, true));
    Schema srcSchema = Schema.recordOf(
      "taybull",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    TableDetail expectedDetail = TableDetail.builder("deebee", "taybull", null)
      .setPrimaryKey(Collections.singletonList("id"))
      .setColumns(columns)
      .build();
    Schema tgtSchema =  Schema.recordOf(
      "taybull",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("renamed", Schema.of(Schema.Type.BOOLEAN)));
    StandardizedTableDetail expectedStandardizedTableDetail =
      new StandardizedTableDetail(expectedDetail.getDatabase(), expectedDetail.getTable(),
                                  expectedDetail.getPrimaryKey(), tgtSchema);
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
    TableAssessment expectedSrcTableAssessment = new TableAssessment(columnAssessments, Collections.emptyList());

    columnAssessments = new ArrayList<>();
    columnAssessments.add(ColumnAssessment.builder("id", "INT").setSourceColumn("id").build());
    columnAssessments.add(ColumnAssessment.builder("renamed", "STRING")
                            .setSourceColumn("renamed")
                            .setSupport(ColumnSupport.NO)
                            .setSuggestion(new ColumnSuggestion("msg", Collections.emptyList()))
                            .build());
    columnAssessments.add(ColumnAssessment.builder("suppressed", "INT")
                            .setSourceColumn("suppressed")
                            .setSupport(ColumnSupport.PARTIAL)
                            .setSuggestion(new ColumnSuggestion("msg", Collections.emptyList()))
                            .build());
    TableAssessment expectedTgtTableAssessment = new TableAssessment(columnAssessments, Collections.emptyList());

    MockTableAssessor srcMockAssessor = new MockTableAssessor(expectedSrcTableAssessment);
    MockTableAssessor tgtMockAssessor = new MockTableAssessor(expectedTgtTableAssessment,
                                                              expectedStandardizedTableDetail);
    MockTableRegistry mockTableRegistry = new MockTableRegistry(expectedList, expectedDetail, srcSchema);
    DeltaSource mockSource = new MockSource(mockTableRegistry, srcMockAssessor);
    DeltaTarget mockTarget = new MockTarget(tgtMockAssessor);

    // rename "name" to "renamed" and change the type to boolean
    Map<String, String> renameMap = new HashMap<>();
    renameMap.put("name", "renamed");

    Map<String, Transformation> transformationsMap = new HashMap<>();
    transformationsMap.put(MockTransformation.NAME, new MockTransformation(Collections.singletonList(Schema.Field.of(
      "name", Schema.of(Schema.Type.BOOLEAN))), renameMap, null));
    Configurer mockConfigurer = new MockConfigurer(mockSource, mockTarget, transformationsMap);

    TableSummaryAssessment summaryAssessment = new TableSummaryAssessment("deebee", "taybull", 3, 1, 0, null);
    PipelineAssessment expected = new PipelineAssessment(Collections.singletonList(summaryAssessment),
                                                         Collections.emptyList(), Collections.emptyList(),
                                                         Collections.emptyList());
    PipelineAssessment actual = service.assessPipeline(namespace, config, mockConfigurer);
    Assert.assertEquals(expected, actual);
  }

  

  @Test
  public void testMacroEvaluation() throws Exception {
    Namespace namespace = new Namespace("ns", 0L);
    DraftId draftId = new DraftId(namespace, "testAssessPipeline");

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
    ConfigMacroEvaluator macroEvaluator = new ConfigMacroEvaluator(propertyEvaluator);

    DeltaConfig config = DeltaConfig.builder()
      .setSource(new Stage("src", new Plugin("mock", DeltaSource.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY)))
      .setTarget(new Stage("tgt", new Plugin("mock", DeltaTarget.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY)))
      .setTables(Collections.singletonList(new SourceTable("deebee", "taybull")))
      .build();

    DraftService service = new DraftService(getTransactionRunner(), macroEvaluator);
    service.saveDraft(draftId, new DraftRequest("label", config));

    List<FullColumnAssessment> fullColumns = columnAssessments.stream()
      .map(c -> new FullColumnAssessment(c.getSupport(), c.getName(), c.getType(), c.getName(), c.getType(),
                                         c.getSuggestion()))
      .collect(Collectors.toList());
    TableAssessmentResponse expectedAssessment = new TableAssessmentResponse(fullColumns, Collections.emptyList(),
                                                                             Collections.emptyList());
    Configurer configurer = new PropertyBasedMockConfigurer();
    Assert.assertEquals(expectedList, service.listDraftTables(draftId, configurer));
    DBTable dbTable = new DBTable("deebee", null, "taybull");
    Assert.assertEquals(expectedDetail, service.describeDraftTable(draftId, configurer, dbTable));
    Assert.assertEquals(expectedAssessment, service.assessTable(namespace, config, configurer,
                                                                new DBTable("deebee", "taybull")));
  }
}
