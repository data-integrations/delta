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
import io.cdap.cdap.common.utils.Tasks;
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
import io.cdap.delta.proto.PipelineReplicationState;
import io.cdap.delta.proto.PipelineState;
import io.cdap.delta.proto.RetryConfig;
import io.cdap.delta.proto.Stage;
import io.cdap.delta.proto.TableReplicationState;
import io.cdap.delta.proto.TableState;
import io.cdap.delta.store.StateStore;
import io.cdap.delta.test.DeltaPipelineTestBase;
import io.cdap.delta.test.mock.FailureTarget;
import io.cdap.delta.test.mock.FileEventConsumer;
import io.cdap.delta.test.mock.MockSource;
import io.cdap.delta.test.mock.MockTarget;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
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

    String offsetBasePath = TEMP_FOLDER.newFolder("testOneRun").getAbsolutePath();
    Stage source = new Stage("src", MockSource.getPlugin(events));
    Stage target = new Stage("target", MockTarget.getPlugin(outputFile));
    DeltaConfig config = DeltaConfig.builder()
      .setSource(source)
      .setTarget(target)
      .setOffsetBasePath(offsetBasePath)
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

    FileSystem fs = FileSystem.get(new Configuration());
    Path path = new Path(offsetBasePath);
    StateStore stateStore = new StateStore(fs, path);
    Long generation = stateStore.getLatestGeneration(appId.getNamespace(), appId.getApplication());
    DeltaPipelineId pipelineId = new DeltaPipelineId(appId.getNamespace(), appId.getApplication(), generation);
    PipelineStateService stateService = new PipelineStateService(pipelineId, stateStore);
    stateService.load();

    OffsetAndSequence offsetAndSequence = stateStore.readOffset(pipelineId);
    Assert.assertEquals(2L, offsetAndSequence.getSequenceNumber());

    TableReplicationState tableState = new TableReplicationState("deebee", "taybull", TableState.REPLICATE, null);
    PipelineReplicationState expectedState = new PipelineReplicationState(PipelineState.OK,
                                                                          Collections.singleton(tableState), null);
    PipelineReplicationState actualState = stateService.getState();
    Assert.assertEquals(expectedState, actualState);
  }

  @Test
  public void testRestartFromOffset() throws Exception {
    File outputFile = TMP_FOLDER.newFile("testRestartFromOffset.json");

    List<ChangeEvent> events = new ArrayList<>();
    events.add(EVENT1);
    events.add(EVENT2);

    String offsetBasePath = TEMP_FOLDER.newFolder("testRestartFromOffset").getAbsolutePath();
    Stage source = new Stage("src", MockSource.getPlugin(events, 1));
    Stage target = new Stage("target", MockTarget.getPlugin(outputFile));
    DeltaConfig config = DeltaConfig.builder()
      .setSource(source)
      .setTarget(target)
      .setOffsetBasePath(offsetBasePath)
      .build();

    AppRequest<DeltaConfig> appRequest = new AppRequest<>(ARTIFACT_SUMMARY, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("testRestartFromOffset");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkerManager manager = appManager.getWorkerManager(DeltaWorker.NAME);
    manager.startAndWaitForRun(ProgramRunStatus.RUNNING, 60, TimeUnit.SECONDS);

    waitForMetric(appId, "target.ddl", 1);
    TimeUnit.SECONDS.sleep(20);
    manager.stop();
    manager.waitForStopped(60, TimeUnit.SECONDS);

    FileSystem fs = FileSystem.get(new Configuration());
    Path path = new Path(offsetBasePath);
    StateStore stateStore = new StateStore(fs, path);
    Long generation = stateStore.getLatestGeneration(appId.getNamespace(), appId.getApplication());
    DeltaPipelineId pipelineId = new DeltaPipelineId(appId.getNamespace(), appId.getApplication(), generation);
    PipelineStateService stateService = new PipelineStateService(pipelineId, stateStore);
    stateService.load();

    OffsetAndSequence offsetAndSequence = stateStore.readOffset(pipelineId);
    Assert.assertEquals(1L, offsetAndSequence.getSequenceNumber());

    // should only have written out the first change
    List<? extends ChangeEvent> actual = FileEventConsumer.readEvents(outputFile);
    List<? extends ChangeEvent> expected = Collections.singletonList(events.get(0));
    Assert.assertEquals(expected, actual);

    // offset should have been saved, with the next run starting from that offset
    manager.startAndWaitForRun(ProgramRunStatus.RUNNING, 60, TimeUnit.SECONDS);

    waitForMetric(appId, "target.dml.insert", 1);
    manager.stop();
    manager.waitForStopped(60, TimeUnit.SECONDS);

    offsetAndSequence = stateStore.readOffset(pipelineId);
    Assert.assertEquals(2L, offsetAndSequence.getSequenceNumber());

    // second change should have been written
    actual = FileEventConsumer.readEvents(outputFile);
    expected = Collections.singletonList(events.get(1));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testFailImmediately() throws Exception {
    List<ChangeEvent> events = new ArrayList<>();
    events.add(EVENT1);
    events.add(EVENT2);

    String offsetBasePath = TEMP_FOLDER.newFolder("testFailImmediately").getAbsolutePath();
    Stage source = new Stage("src", MockSource.getPlugin(events));

    // configure the target to throw exceptions after the first event is applied
    // until the proceedFile is created
    Stage target = new Stage("target", FailureTarget.failImmediately(1L));
    DeltaConfig config = DeltaConfig.builder()
      .setSource(source)
      .setTarget(target)
      .setOffsetBasePath(offsetBasePath)
      // configure it to retry indefinitely.
      // The run will only finish because of a failure and not from retry exhaustion
      .setRetryConfig(new RetryConfig(Integer.MAX_VALUE, 0))
      .build();

    AppRequest<DeltaConfig> appRequest = new AppRequest<>(ARTIFACT_SUMMARY, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("testFailImmediately");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkerManager manager = appManager.getWorkerManager(DeltaWorker.NAME);
    manager.startAndWaitForRun(ProgramRunStatus.FAILED, 60, TimeUnit.SECONDS);
  }

  @Test
  public void testFailureRetries() throws Exception {
    String proceedFilePath = TMP_FOLDER.getRoot().getAbsolutePath();
    File proceedFile = new File(proceedFilePath, "proceed");

    List<ChangeEvent> events = new ArrayList<>();
    events.add(EVENT1);
    events.add(EVENT2);

    String offsetBasePath = TEMP_FOLDER.newFolder("testFailureRetries").getAbsolutePath();
    Stage source = new Stage("src", MockSource.getPlugin(events));

    // configure the target to throw exceptions after the first event is applied
    // until the proceedFile is created
    Stage target = new Stage("target", FailureTarget.failAfter(1L, proceedFile));
    DeltaConfig config = DeltaConfig.builder()
      .setSource(source)
      .setTarget(target)
      .setOffsetBasePath(offsetBasePath)
      .setRetryConfig(new RetryConfig(300, 0))
      .build();

    AppRequest<DeltaConfig> appRequest = new AppRequest<>(ARTIFACT_SUMMARY, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("testFailureRetries");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkerManager manager = appManager.getWorkerManager(DeltaWorker.NAME);
    manager.startAndWaitForRun(ProgramRunStatus.RUNNING, 60, TimeUnit.SECONDS);

    // wait for the 1st event to be applied, after which the target should start throwing exceptions
    waitForMetric(appId, "target.ddl", 1);

    // wait for the replication state for the table to be set to ERROR.
    FileSystem fs = FileSystem.get(new Configuration());
    Path path = new Path(offsetBasePath);
    StateStore stateStore = new StateStore(fs, path);
    Long generation = stateStore.getLatestGeneration(appId.getNamespace(), appId.getApplication());
    DeltaPipelineId pipelineId = new DeltaPipelineId(appId.getNamespace(), appId.getApplication(), generation);
    PipelineStateService stateService = new PipelineStateService(pipelineId, stateStore);
    Tasks.waitFor(true, () -> {
      stateService.load();
      for (TableReplicationState state : stateService.getState().getTables()) {
        if (EVENT2.getDatabase().equals(state.getDatabase()) &&
          EVENT2.getTable().equals(state.getTable()) && state.getState() == TableState.ERROR) {
          return true;
        }
      }
      return false;
    }, 30, TimeUnit.SECONDS);

    // create the proceed file, which will tell the target to stop throwing exceptions
    proceedFile.createNewFile();

    // wait for replication state for the table to update to REPLICATING
    Tasks.waitFor(true, () -> {
      stateService.load();
      for (TableReplicationState state : stateService.getState().getTables()) {
        if (EVENT2.getDatabase().equals(state.getDatabase()) &&
          EVENT2.getTable().equals(state.getTable()) && state.getState() == TableState.REPLICATE) {
          return true;
        }
      }
      return false;
    }, 30, TimeUnit.SECONDS);

    manager.stop();
    manager.waitForStopped(60, TimeUnit.SECONDS);

    // verify that the sequence number was correctly being rolled back during errors and not incremented
    OffsetAndSequence offsetAndSequence = stateStore.readOffset(pipelineId);
    Assert.assertEquals(2L, offsetAndSequence.getSequenceNumber());

    // verify that metrics were not double counted during errors
    waitForMetric(appId, "target.ddl", 1);
    waitForMetric(appId, "target.dml.insert", 1);

    Assert.assertEquals(1, manager.getHistory(ProgramRunStatus.KILLED).size());
    // check that state is killed and not failed
    Assert.assertEquals(1, manager.getHistory(ProgramRunStatus.KILLED).size());
  }

  private void waitForMetric(ApplicationId appId, String metric, int expected)
    throws TimeoutException, InterruptedException, ExecutionException {
    Map<String, String> tags = new HashMap<>();
    tags.put(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace());
    tags.put(Constants.Metrics.Tag.APP, appId.getEntityName());
    tags.put(Constants.Metrics.Tag.WORKER, DeltaWorker.NAME);
    // use this instead of getMetricsManager().waitForTotalMetricCount(), because that method will
    // allow the metric to be higher than the passed in count
    Tasks.waitFor((long) expected,
                  (Callable<Long>) () -> getMetricsManager().getTotalMetric(tags, "user." + metric),
                  30, TimeUnit.SECONDS);
  }
}
