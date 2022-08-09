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

import io.cdap.cdap.api.app.ApplicationUpdateResult;
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
import io.cdap.delta.api.DeltaPipelineId;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaTarget;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.proto.Artifact;
import io.cdap.delta.proto.DeltaConfig;
import io.cdap.delta.proto.ParallelismConfig;
import io.cdap.delta.proto.PipelineReplicationState;
import io.cdap.delta.proto.PipelineState;
import io.cdap.delta.proto.Plugin;
import io.cdap.delta.proto.RetryConfig;
import io.cdap.delta.proto.Stage;
import io.cdap.delta.proto.TableReplicationState;
import io.cdap.delta.proto.TableState;
import io.cdap.delta.store.StateStore;
import io.cdap.delta.test.DeltaPipelineTestBase;
import io.cdap.delta.test.mock.FailureTarget;
import io.cdap.delta.test.mock.FileEventConsumer;
import io.cdap.delta.test.mock.MockApplicationUpdateContext;
import io.cdap.delta.test.mock.MockErrorTarget;
import io.cdap.delta.test.mock.MockSource;
import io.cdap.delta.test.mock.MockTarget;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests for delta pipelines
 */
public class DeltaPipelineTest extends DeltaPipelineTestBase {
  private static final String DATABASE = "deebee";
  private static final String TABLE = "taybull";
  private static final String TABLE2 = "taybull2";

  private static final Schema SCHEMA = Schema.recordOf(TABLE, Schema.Field.of("id", Schema.of(Schema.Type.INT)));

  private static final DDLEvent EVENT1 = DDLEvent.builder()
    .setOffset(new Offset(Collections.singletonMap("order", "0")))
    .setOperation(DDLOperation.Type.CREATE_TABLE)
    .setDatabaseName(DATABASE)
    .setTableName(TABLE)
    .setPrimaryKey(Collections.singletonList("id"))
    .setSchema(SCHEMA)
    .build();

  private static final DDLEvent EVENT3 = DDLEvent.builder()
    .setOffset(new Offset(Collections.singletonMap("order", "2")))
    .setOperation(DDLOperation.Type.CREATE_TABLE)
    .setDatabaseName(DATABASE)
    .setTableName(TABLE2)
    .setPrimaryKey(Collections.singletonList("id"))
    .setSchema(SCHEMA)
    .build();

  private static DMLEvent createDMLEvent() {
    // set ingest timestamp for the event to be 2 seconds in the past so that there will always be
    // non-zero latency metric "dml.latency.seconds" in processing this event. Without this delay its possible
    // that event is processed within same second at target side causing latency 0
    long ingestTimeStamp = System.currentTimeMillis() - 2000;
    return DMLEvent.builder()
      .setOffset(new Offset(Collections.singletonMap("order", "1")))
      .setOperationType(DMLOperation.Type.INSERT)
      .setDatabaseName("deebee")
      .setTableName(TABLE)
      .setIngestTimestamp(ingestTimeStamp)
      .setRow(StructuredRecord.builder(SCHEMA).set("id", 0).build())
      .build();
  }
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  @BeforeClass
  public static void setupTest() throws Exception {
    setupArtifacts(DeltaApp.class);
  }

  @AfterClass
  public static void teardown() throws Exception {
    removeArtifacts();
  }

  @Test
  public void testOneRun() throws Exception {
    File outputFolder = TMP_FOLDER.newFolder("testOneRun");

    List<ChangeEvent> events = new ArrayList<>();
    events.add(EVENT1);
    events.add(createDMLEvent());

    String offsetBasePath = outputFolder.getAbsolutePath();
    Stage source = new Stage("src", MockSource.getPlugin(events));
    Stage target = new Stage("target", MockTarget.getPlugin(outputFolder));
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

    waitForMetric(appId, "ddl", 1);
    waitForMetric(appId, "dml.inserts", 1);
    // Latency should be somewhere between 1 seconds and 30 seconds
    waitForMetric(appId, "dml.latency.seconds", 1, 30);
    manager.stop();
    manager.waitForStopped(60, TimeUnit.SECONDS);

    List<? extends ChangeEvent> actual = FileEventConsumer.readEvents(outputFolder, 0);
    Assert.assertEquals(events, actual);

    Path path = new Path(offsetBasePath);
    StateStore stateStore = StateStore.from(path);
    Long generation = stateStore.getLatestGeneration(appId.getNamespace(), appId.getApplication());
    DeltaPipelineId pipelineId = new DeltaPipelineId(appId.getNamespace(), appId.getApplication(), generation);
    DeltaWorkerId workerId = new DeltaWorkerId(pipelineId, 0);
    PipelineStateService stateService = new PipelineStateService(workerId, stateStore);
    stateService.load();

    OffsetAndSequence offsetAndSequence = stateStore.readOffset(workerId);
    Assert.assertEquals(1L, offsetAndSequence.getSequenceNumber());

    TableReplicationState tableState = new TableReplicationState(DATABASE, TABLE, TableState.REPLICATING, null);
    PipelineReplicationState expectedState = new PipelineReplicationState(PipelineState.OK,
                                                                          Collections.singleton(tableState), null);
    PipelineReplicationState actualState = stateService.getState();
    Assert.assertEquals(expectedState, actualState);
  }

  @Test
  public void testRestartFromOffset() throws Exception {
    File outputFolder = TMP_FOLDER.newFolder("testRestartFromOffset");

    List<ChangeEvent> events = new ArrayList<>();
    events.add(EVENT1);
    events.add(createDMLEvent());

    String offsetBasePath = outputFolder.getAbsolutePath();
    Stage source = new Stage("src", MockSource.getPlugin(events, 1));
    Stage target = new Stage("target", MockTarget.getPlugin(outputFolder));
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

    waitForMetric(appId, "ddl", 1);
    TimeUnit.SECONDS.sleep(20);
    manager.stop();
    manager.waitForStopped(60, TimeUnit.SECONDS);

    Path path = new Path(offsetBasePath);
    StateStore stateStore = StateStore.from(path);
    Long generation = stateStore.getLatestGeneration(appId.getNamespace(), appId.getApplication());
    DeltaPipelineId pipelineId = new DeltaPipelineId(appId.getNamespace(), appId.getApplication(), generation);
    DeltaWorkerId workerId = new DeltaWorkerId(pipelineId, 0);
    PipelineStateService stateService = new PipelineStateService(workerId, stateStore);
    stateService.load();

    OffsetAndSequence offsetAndSequence = stateStore.readOffset(workerId);
    Assert.assertEquals(0L, offsetAndSequence.getSequenceNumber());

    // should only have written out the first change
    List<? extends ChangeEvent> actual = FileEventConsumer.readEvents(outputFolder, 0);
    List<? extends ChangeEvent> expected = Collections.singletonList(events.get(0));
    Assert.assertEquals(expected, actual);

    // offset should have been saved, with the next run starting from that offset
    manager.startAndWaitForRun(ProgramRunStatus.RUNNING, 60, TimeUnit.SECONDS);

    waitForMetric(appId, "dml.inserts", 1);
    // Latency should be somewhere between 1 seconds and 30 seconds
    waitForMetric(appId, "dml.latency.seconds", 1, 40);
    manager.stop();
    manager.waitForStopped(60, TimeUnit.SECONDS);

    offsetAndSequence = stateStore.readOffset(workerId);
    Assert.assertEquals(1L, offsetAndSequence.getSequenceNumber());

    // second change should have been written
    actual = FileEventConsumer.readEvents(outputFolder, 0);
    expected = Collections.singletonList(events.get(1));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testFailImmediately() throws Exception {
    List<ChangeEvent> events = new ArrayList<>();
    events.add(EVENT1);
    events.add(createDMLEvent());

    String offsetBasePath = TMP_FOLDER.newFolder("testFailImmediately").getAbsolutePath();
    Stage source = new Stage("src", MockSource.getPlugin(events));

    // configure the target to throw exceptions after the first event is applied
    // until the proceedFile is created
    Stage target = new Stage("target", FailureTarget.failImmediately(0L));
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
  public void testStopWhenFailing() throws Exception {
    String tempFolderPath = TMP_FOLDER.getRoot().getAbsolutePath();
    File sourceProceedFile = new File(tempFolderPath, "sourceProceed");
    File outputFolder = TMP_FOLDER.newFolder();

    List<ChangeEvent> events = new ArrayList<>();
    events.add(EVENT1);
    events.add(createDMLEvent());

    String offsetBasePath = TMP_FOLDER.newFolder("testStopWhenFailing").getAbsolutePath();
    Stage source = new Stage("src", MockSource.getPlugin(events, sourceProceedFile));

    // configure the target to throw exceptions after the first event is applied
    // until the proceedFile is created
    Stage target = new Stage("target", MockTarget.getPlugin(outputFolder));
    DeltaConfig config = DeltaConfig.builder()
      .setSource(source)
      .setTarget(target)
      .setOffsetBasePath(offsetBasePath)
      .setRetryConfig(new RetryConfig(300, 0))
      .build();

    AppRequest<DeltaConfig> appRequest = new AppRequest<>(ARTIFACT_SUMMARY, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("testStopWhenFailing");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkerManager manager = appManager.getWorkerManager(DeltaWorker.NAME);
    manager.startAndWaitForRun(ProgramRunStatus.RUNNING, 60, TimeUnit.SECONDS);

    // wait for the replication state for the source to be set to ERROR
    Path path = new Path(offsetBasePath);
    StateStore stateStore = StateStore.from(path);
    Tasks.waitFor(true, () -> stateStore.getLatestGeneration(appId.getNamespace(), appId.getApplication()) != null,
                  1, TimeUnit.MINUTES);
    Long generation = stateStore.getLatestGeneration(appId.getNamespace(), appId.getApplication());
    DeltaPipelineId pipelineId = new DeltaPipelineId(appId.getNamespace(), appId.getApplication(), generation);
    DeltaWorkerId workerId = new DeltaWorkerId(pipelineId, 0);
    PipelineStateService stateService = new PipelineStateService(workerId, stateStore);
    Tasks.waitFor(PipelineState.FAILING, () -> {
      stateService.load();
      return stateService.getState().getSourceState();
    }, 30, TimeUnit.SECONDS);

    // stop the worker to make sure it aborts the retry loop
    manager.stop();
    manager.waitForStopped(60, TimeUnit.SECONDS);
  }

  @Test
  public void testFailureRetries() throws Exception {
    String tempFolderPath = TMP_FOLDER.getRoot().getAbsolutePath();
    File sourceProceedFile = new File(tempFolderPath, "sourceProceed");
    File targetProceedFile = new File(tempFolderPath, "targetProceed");

    List<ChangeEvent> events = new ArrayList<>();
    events.add(EVENT1);
    events.add(createDMLEvent());

    String offsetBasePath = TMP_FOLDER.newFolder("testFailureRetries").getAbsolutePath();
    Stage source = new Stage("src", MockSource.getPlugin(events, sourceProceedFile));

    // configure the target to throw exceptions after the first event is applied
    // until the proceedFile is created
    Stage target = new Stage("target", FailureTarget.failAfter(0L, targetProceedFile));
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

    // wait for the replication state for the source to be set to ERROR
    Path path = new Path(offsetBasePath);
    StateStore stateStore = StateStore.from(path);
    Tasks.waitFor(true, () -> stateStore.getLatestGeneration(appId.getNamespace(), appId.getApplication()) != null,
                  1, TimeUnit.MINUTES);
    Long generation = stateStore.getLatestGeneration(appId.getNamespace(), appId.getApplication());
    DeltaPipelineId pipelineId = new DeltaPipelineId(appId.getNamespace(), appId.getApplication(), generation);
    DeltaWorkerId workerId = new DeltaWorkerId(pipelineId, 0);
    PipelineStateService stateService = new PipelineStateService(workerId, stateStore);
    Tasks.waitFor(PipelineState.FAILING, () -> {
      stateService.load();
      return stateService.getState().getSourceState();
    }, 30, TimeUnit.SECONDS);

    // create the proceed file for the source, which will tell the source to stop throwing exceptions
    sourceProceedFile.createNewFile();

    // wait for the 1st event to be applied, after which the target should start throwing exceptions
    waitForMetric(appId, "ddl", 1);

    // wait for the replication state for the table to be set to ERROR.
    Tasks.waitFor(true, () -> {
      stateService.load();
      PipelineReplicationState pipelineState = stateService.getState();
      if (pipelineState.getSourceState() != PipelineState.OK) {
        return false;
      }
      for (TableReplicationState state : pipelineState.getTables()) {
        if (DATABASE.equals(state.getDatabase()) &&
          TABLE.equals(state.getTable()) && state.getState() == TableState.FAILING) {
          return true;
        }
      }
      return false;
    }, 30, TimeUnit.SECONDS);

    // create the proceed file, which will tell the target to stop throwing exceptions
    targetProceedFile.createNewFile();

    // wait for replication state for the table to update to REPLICATING
    Tasks.waitFor(true, () -> {
      stateService.load();
      for (TableReplicationState state : stateService.getState().getTables()) {
        if (DATABASE.equals(state.getDatabase()) &&
          TABLE.equals(state.getTable()) && state.getState() == TableState.REPLICATING) {
          return true;
        }
      }
      return false;
    }, 30, TimeUnit.SECONDS);

    manager.stop();
    manager.waitForStopped(60, TimeUnit.SECONDS);

    // verify that the sequence number was correctly being rolled back during errors and not incremented
    OffsetAndSequence offsetAndSequence = stateStore.readOffset(workerId);
    Assert.assertEquals(1L, offsetAndSequence.getSequenceNumber());

    // verify that metrics were not double counted during errors
    waitForMetric(appId, "ddl", 1);
    waitForMetric(appId, "dml.inserts", 1);
    // Latency should be somewhere between 1 seconds and 30 seconds
    waitForMetric(appId, "dml.latency.seconds", 1, 30);

    Assert.assertEquals(1, manager.getHistory(ProgramRunStatus.KILLED).size());
    // check that state is killed and not failed
    Assert.assertEquals(1, manager.getHistory(ProgramRunStatus.KILLED).size());
  }

  @Test
  public void testMultipleInstances() throws Exception {
    File outputFolder = TMP_FOLDER.newFolder("testMultipleInstances");

    DMLEvent dmlEvent = createDMLEvent();
    List<ChangeEvent> events = new ArrayList<>();
    events.add(EVENT1);
    events.add(dmlEvent);
    events.add(EVENT3);

    String offsetBasePath = outputFolder.getAbsolutePath();
    Stage source = new Stage("src", MockSource.getPlugin(events));
    Stage target = new Stage("target", MockTarget.getPlugin(outputFolder));
    DeltaConfig config = DeltaConfig.builder()
      .setSource(source)
      .setTarget(target)
      .setOffsetBasePath(offsetBasePath)
      // configure instance 0 to read taybull and instance1 to read taybull2
      .setTables(Arrays.asList(new SourceTable(DATABASE, TABLE), new SourceTable(DATABASE, TABLE2)))
      .setParallelism(new ParallelismConfig(2))
      .build();

    AppRequest<DeltaConfig> appRequest = new AppRequest<>(ARTIFACT_SUMMARY, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("testMultipleInstances");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkerManager manager = appManager.getWorkerManager(DeltaWorker.NAME);
    manager.startAndWaitForRun(ProgramRunStatus.RUNNING, 60, TimeUnit.SECONDS);

    waitForMetric(appId, "ddl", 2);
    waitForMetric(appId, "dml.inserts", 1);
    // Latency should be somewhere between 1 seconds and 30 seconds
    waitForMetric(appId, "dml.latency.seconds", 1, 30);
    manager.stop();
    manager.waitForStopped(60, TimeUnit.SECONDS);

    // check events written for instance 0
    List<? extends ChangeEvent> expected = Arrays.asList(EVENT1, dmlEvent);
    List<? extends ChangeEvent> actual = FileEventConsumer.readEvents(outputFolder, 0);
    Assert.assertEquals(expected, actual);

    // check events written for instance 1
    expected = Collections.singletonList(EVENT3);
    actual = FileEventConsumer.readEvents(outputFolder, 1);
    Assert.assertEquals(expected, actual);

    Path path = new Path(offsetBasePath);
    StateStore stateStore = StateStore.from(path);
    Long generation = stateStore.getLatestGeneration(appId.getNamespace(), appId.getApplication());
    DeltaPipelineId pipelineId = new DeltaPipelineId(appId.getNamespace(), appId.getApplication(), generation);
    Collection<Integer> instanceIds = stateStore.getWorkerInstances(pipelineId);
    Assert.assertEquals(2, instanceIds.size());
    Assert.assertTrue(instanceIds.contains(0));
    Assert.assertTrue(instanceIds.contains(1));

    // check pipeline state for instance 0
    DeltaWorkerId workerId = new DeltaWorkerId(pipelineId, 0);
    PipelineStateService stateService = new PipelineStateService(workerId, stateStore);
    stateService.load();

    OffsetAndSequence offsetAndSequence = stateStore.readOffset(workerId);
    Assert.assertEquals(1L, offsetAndSequence.getSequenceNumber());

    TableReplicationState tableState = new TableReplicationState(DATABASE, TABLE, TableState.REPLICATING, null);
    PipelineReplicationState expectedState = new PipelineReplicationState(PipelineState.OK,
                                                                          Collections.singleton(tableState), null);
    PipelineReplicationState actualState = stateService.getState();
    Assert.assertEquals(expectedState, actualState);

    // check pipeline state for instance 1
    workerId = new DeltaWorkerId(pipelineId, 1);
    stateService = new PipelineStateService(workerId, stateStore);
    stateService.load();

    offsetAndSequence = stateStore.readOffset(workerId);
    Assert.assertEquals(0L, offsetAndSequence.getSequenceNumber());

    tableState = new TableReplicationState(DATABASE, TABLE2, TableState.REPLICATING, null);
    expectedState = new PipelineReplicationState(PipelineState.OK, Collections.singleton(tableState), null);
    actualState = stateService.getState();
    Assert.assertEquals(expectedState, actualState);
  }

  @Test
  public void testDataSizeAndErrorMetric() throws Exception {
    Schema dmlSchema = Schema.recordOf(TABLE, Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                       Schema.Field.of("f1", Schema.of(Schema.Type.BOOLEAN)),
                                       Schema.Field.of("f2", Schema.of(Schema.Type.INT)),
                                       Schema.Field.of("f3", Schema.of(Schema.Type.FLOAT)),
                                       Schema.Field.of("f4", Schema.enumWith("enum1", "enum2")),
                                       Schema.Field.of("f5", Schema.of(Schema.Type.LONG)),
                                       Schema.Field.of("f6", Schema.of(Schema.Type.DOUBLE)),
                                       Schema.Field.of("f7", Schema.of(Schema.Type.BYTES)),
                                       Schema.Field.of("f8", Schema.of(Schema.Type.BYTES)),
                                       Schema.Field.of("f9", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
                                       Schema.Field.of("f10", Schema.of(Schema.Type.STRING)));


    DDLEvent ddlEvent = DDLEvent.builder()
      .setOffset(new Offset(Collections.singletonMap("order", "0")))
      .setOperation(DDLOperation.Type.CREATE_TABLE)
      .setDatabaseName(DATABASE)
      .setTableName(TABLE)
      .setPrimaryKey(Collections.singletonList("id"))
      .setSchema(dmlSchema)
      .build();

    File outputFolder = TMP_FOLDER.newFolder("testDataSizeMetric");

    DMLEvent dmlEvent = DMLEvent.builder()
      .setOffset(new Offset(Collections.singletonMap("order", "1")))
      .setOperationType(DMLOperation.Type.INSERT)
      .setDatabaseName(DATABASE)
      .setTableName(TABLE)
      .setIngestTimestamp(System.currentTimeMillis())
      .setRow(StructuredRecord.builder(dmlSchema).set("id", 0) // 4 bytes for INT
                .set("f1", true) // 1 byte for BOOLEAN
                .set("f2", 1) // 4 bytes for INT
                .set("f3", 1f) // 4 bytes for FLOAT
                .set("f4", "enum1") // 4 bytes for ENUM
                .set("f5", 2L) // 8 bytes for LONG
                .set("f6", 10.0) // 8 bytes for DOUBLE
                .set("f7", new byte[10]) // 10 bytes for byte[]
                .set("f8", ByteBuffer.allocate(10)) // 10 bytes for ByteBuffer
                .set("f10", "some string") // 13 bytes: "\"some string\"".getBytes(StandardCharsets.UTF_8).length
                .build()) // total 64 bytes
      .build();

    List<ChangeEvent> events = new ArrayList<>();
    events.add(ddlEvent);
    events.add(dmlEvent);

    String offsetBasePath = outputFolder.getAbsolutePath();
    Stage source = new Stage("src", MockSource.getPlugin(events));
    Stage target = new Stage("target", MockTarget.getPlugin(outputFolder, true));
    DeltaConfig config = DeltaConfig.builder()
      .setSource(source)
      .setTarget(target)
      .setOffsetBasePath(offsetBasePath)
      // configure instance 0 to read taybull and instance1 to read taybull2
      .setTables(Arrays.asList(new SourceTable(DATABASE, TABLE), new SourceTable(DATABASE, TABLE2)))
      .setParallelism(new ParallelismConfig(2))
      .build();

    AppRequest<DeltaConfig> appRequest = new AppRequest<>(ARTIFACT_SUMMARY, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("testDataSizeAndErrorMetric");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkerManager manager = appManager.getWorkerManager(DeltaWorker.NAME);
    manager.startAndWaitForRun(ProgramRunStatus.RUNNING, 60, TimeUnit.SECONDS);

    waitForMetric(appId, "dml.data.processed.bytes", 64);
    waitForMetric(appId, "dml.errors", 1);
    manager.stop();
    manager.waitForStopped(60, TimeUnit.SECONDS);
  }

  private void waitForMetric(ApplicationId appId, String metric, int expected)
    throws TimeoutException, InterruptedException, ExecutionException {
    Map<String, String> tags = createMetricTags(appId);
    // use this instead of getMetricsManager().waitForTotalMetricCount(), because that method will
    // allow the metric to be higher than the passed in count
    Tasks.waitFor((long) expected,
                  () -> getMetricsManager().getTotalMetric(tags, "user." + metric),
                  30, TimeUnit.SECONDS);
  }

  @Test
  public void testErrorOnlyMetric() throws Exception {
    File outputFolder = TMP_FOLDER.newFolder("testErrorOnlyMetric");

    List<ChangeEvent> events = new ArrayList<>();
    events.add(EVENT1);
    events.add(createDMLEvent());
    events.add(createDMLEvent());

    String offsetBasePath = outputFolder.getAbsolutePath();
    Stage source = new Stage("src", MockSource.getPlugin(events));
    Stage target = new Stage("target", MockErrorTarget.getPlugin());
    DeltaConfig config = DeltaConfig.builder()
      .setSource(source)
      .setTarget(target)
      .setOffsetBasePath(offsetBasePath)
      .build();

    AppRequest<DeltaConfig> appRequest = new AppRequest<>(ARTIFACT_SUMMARY, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("testErrorOnlyMetric");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkerManager manager = appManager.getWorkerManager(DeltaWorker.NAME);
    manager.startAndWaitForRun(ProgramRunStatus.RUNNING, 60, TimeUnit.SECONDS);

    waitForMetric(appId, "dml.errors", 2);
    manager.stop();
    manager.waitForStopped(60, TimeUnit.SECONDS);
  }

  @Test
  public void testApplicationUpgrade() throws Exception {
    Artifact originalSource = new Artifact("mysql-connector", "0.3.0-SNAPSHOT", "SYSTEM");
    Artifact originalTarget = new Artifact("bq-connector", "0.3.0-SNAPSHOT", "SYSTEM");

    // Upgraded artifacts have version bumped
    Artifact upgradedSource = new Artifact("mysql-connector", "0.3.1-SNAPSHOT", "SYSTEM");
    Artifact upgradedTarget = new Artifact("bq-connector", "0.3.1-SNAPSHOT", "SYSTEM");
    Stage source = new Stage("source", new Plugin("source", DeltaSource.PLUGIN_TYPE, Collections.emptyMap(),
                                                  originalSource));
    Stage target = new Stage("target", new Plugin("target", DeltaTarget.PLUGIN_TYPE, Collections.emptyMap(),
                                                  originalTarget));

    DeltaConfig originalConfig = DeltaConfig.builder().setSource(source).setTarget(target).build();
    MockApplicationUpdateContext updateContext = new MockApplicationUpdateContext(originalConfig, upgradedSource,
                                                                                  upgradedTarget);
    ApplicationUpdateResult<DeltaConfig> updatedDeltaConfig = new DeltaApp().updateConfig(updateContext);
    List<Stage> stages = updatedDeltaConfig.getNewConfig().getStages();
    Assert.assertEquals(2, stages.size());
    for (Stage stage : stages) {
      switch (stage.getPlugin().getType()) {
        case DeltaSource.PLUGIN_TYPE:
          Assert.assertEquals(upgradedSource, stage.getPlugin().getArtifact());
          break;
        case DeltaTarget.PLUGIN_TYPE:
          Assert.assertEquals(upgradedTarget, stage.getPlugin().getArtifact());
          break;
        default:
          Assert.fail("Unknown plugin type found!");
      }
    }
  }

  private void waitForMetric(ApplicationId appId, String metric, long lowerBound, long upperBound)
    throws InterruptedException, ExecutionException, TimeoutException {
    Map<String, String> tags = createMetricTags(appId);
    Tasks.waitFor(true,
                  () -> {
                    long value = getMetricsManager().getTotalMetric(tags, "user." + metric);

                    String msg = String.format("Got value %s for metric %s, expected in between %s and %s",
                                               value, metric, lowerBound, upperBound);
                    System.out.println(msg);
                    return value >= lowerBound && value <= upperBound;
                  },
                  60, TimeUnit.SECONDS);
  }

  private Map<String, String> createMetricTags(ApplicationId appId) {
    Map<String, String> tags = new HashMap<>();
    tags.put(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace());
    tags.put(Constants.Metrics.Tag.APP, appId.getEntityName());
    tags.put(Constants.Metrics.Tag.WORKER, DeltaWorker.NAME);
    return tags;
  }
}
