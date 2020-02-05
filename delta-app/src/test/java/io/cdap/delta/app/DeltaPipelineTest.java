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

package io.cdap.delta.app;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkerManager;
import io.cdap.delta.api.ChangeEvent;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.Offset;
import io.cdap.delta.proto.DeltaConfig;
import io.cdap.delta.proto.Stage;
import io.cdap.delta.test.DeltaPipelineTestBase;
import io.cdap.delta.test.mock.FileEventConsumer;
import io.cdap.delta.test.mock.MockSource;
import io.cdap.delta.test.mock.MockTarget;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests for delta pipelines
 */
public class DeltaPipelineTest extends DeltaPipelineTestBase {
  private static final Schema SCHEMA = Schema.recordOf("taybull", Schema.Field.of("id", Schema.of(Schema.Type.INT)));
  private static final DDLEvent EVENT1 = DDLEvent.builder()
    .setOffset(new Offset(Collections.singletonMap("order", new byte[] { 0 })))
    .setOperation(DDLOperation.CREATE_TABLE)
    .setDatabase("deebee")
    .setTable("taybull")
    .setPrimaryKey(Collections.singletonList("id"))
    .setSchema(SCHEMA)
    .build();
  private static final DMLEvent EVENT2 = DMLEvent.builder()
    .setOffset(new Offset(Collections.singletonMap("order", new byte[] { 1 })))
    .setOperation(DMLOperation.INSERT)
    .setDatabase("deebee")
    .setTable("taybull")
    .setIngestTimestamp(1000L)
    .setRow(StructuredRecord.builder(SCHEMA).set("id", 0).build())
    .build();

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @BeforeClass
  public static void setupTest() throws Exception {
    setupArtifacts(DeltaApp.class);
  }

  @Test
  public void testOneRun() throws Exception {
    File outputFile = TMP_FOLDER.newFile("testOneRun.json");

    List<ChangeEvent> events = new ArrayList<>();
    events.add(EVENT1);
    events.add(EVENT2);

    Stage source = new Stage("src", MockSource.getPlugin(events));
    Stage target = new Stage("target", MockTarget.getPlugin(outputFile));
    DeltaConfig config = DeltaConfig.builder()
      .setSource(source)
      .setTarget(target)
      .setOffsetBasePath(TEMP_FOLDER.newFolder("testOneRunOffsets").getAbsolutePath())
      .build();

    AppRequest<DeltaConfig> appRequest = new AppRequest<>(ARTIFACT_SUMMARY, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("testOneRun");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkerManager manager = appManager.getWorkerManager(DeltaWorker.NAME);
    manager.startAndWaitForRun(ProgramRunStatus.RUNNING, 60, TimeUnit.SECONDS);

    waitForMetric(appId, "target.ddl", 1);
    waitForMetric(appId, "target.dml.insert", 1);
    manager.stop();
    manager.waitForStopped(60, TimeUnit.SECONDS);

    List<? extends ChangeEvent> actual = FileEventConsumer.readEvents(outputFile);
    Assert.assertEquals(events, actual);
  }

  @Test
  public void testRestartFromOffset() throws Exception {
    File outputFile = TMP_FOLDER.newFile("testRestartFromOffset.json");

    List<ChangeEvent> events = new ArrayList<>();
    events.add(EVENT1);
    events.add(EVENT2);

    Stage source = new Stage("src", MockSource.getPlugin(events, 1));
    Stage target = new Stage("target", MockTarget.getPlugin(outputFile));
    DeltaConfig config = DeltaConfig.builder()
      .setSource(source)
      .setTarget(target)
      .setOffsetBasePath(TEMP_FOLDER.newFolder("testRestartFromOffset").getAbsolutePath())
      .build();

    AppRequest<DeltaConfig> appRequest = new AppRequest<>(ARTIFACT_SUMMARY, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("testRestartFromOffset");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkerManager manager = appManager.getWorkerManager(DeltaWorker.NAME);
    manager.startAndWaitForRun(ProgramRunStatus.RUNNING, 60, TimeUnit.SECONDS);

    waitForMetric(appId, "target.ddl", 1);
    manager.stop();
    manager.waitForStopped(60, TimeUnit.SECONDS);

    // should only have written out the first change
    List<? extends ChangeEvent> actual = FileEventConsumer.readEvents(outputFile);
    List<? extends ChangeEvent> expected = Collections.singletonList(events.get(0));
    Assert.assertEquals(expected, actual);

    // offset should have been saved, with the next run starting from that offset
    manager.startAndWaitForRun(ProgramRunStatus.RUNNING, 60, TimeUnit.SECONDS);

    waitForMetric(appId, "target.dml.insert", 1);
    manager.stop();
    manager.waitForStopped(60, TimeUnit.SECONDS);

    // second change should have been written
    actual = FileEventConsumer.readEvents(outputFile);
    expected = Collections.singletonList(events.get(1));
    Assert.assertEquals(expected, actual);
  }

  private void waitForMetric(ApplicationId appId, String metric, int expected)
    throws TimeoutException, InterruptedException {
    Map<String, String> tags = new HashMap<>();
    tags.put(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace());
    tags.put(Constants.Metrics.Tag.APP, appId.getEntityName());
    tags.put(Constants.Metrics.Tag.WORKER, DeltaWorker.NAME);
    getMetricsManager().waitForTotalMetricCount(tags, "user." + metric, expected, 20, TimeUnit.SECONDS);
  }
}
