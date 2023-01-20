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

import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.worker.WorkerConfigurer;
import io.cdap.cdap.api.worker.WorkerContext;
import io.cdap.cdap.api.worker.WorkerSpecification;
import io.cdap.cdap.app.DefaultAppConfigurer;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.delta.api.DeltaFailureRuntimeException;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaTarget;
import io.cdap.delta.api.EventConsumer;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.proto.Artifact;
import io.cdap.delta.proto.DeltaConfig;
import io.cdap.delta.proto.InstanceConfig;
import io.cdap.delta.proto.ParallelismConfig;
import io.cdap.delta.proto.Plugin;
import io.cdap.delta.proto.Stage;
import io.cdap.delta.proto.TableId;
import io.cdap.delta.store.RemoteStateStore;
import io.cdap.delta.store.StateStoreMigrator;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DeltaWorker}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({WorkerSpecification.class, DefaultApplicationSpecification.class, DeltaWorker.class})
public class DeltaWorkerTest {
  @Rule
  final ExpectedException exception = ExpectedException.none();
  private static final SourceTable TABLE1 = new SourceTable("db", "t1");
  private static final SourceTable TABLE2 = new SourceTable("db", "t2");
  private static final SourceTable TABLE3 = new SourceTable("db", "t3");
  private static final SourceTable TABLE4 = new SourceTable("db", "t4");
  private static final TableId TABLE1_ID = new TableId(TABLE1.getDatabase(), TABLE1.getTable(), TABLE1.getSchema());
  private static final TableId TABLE2_ID = new TableId(TABLE2.getDatabase(), TABLE2.getTable(), TABLE2.getSchema());
  private static final TableId TABLE3_ID = new TableId(TABLE3.getDatabase(), TABLE3.getTable(), TABLE3.getSchema());
  private static final TableId TABLE4_ID = new TableId(TABLE4.getDatabase(), TABLE4.getTable(), TABLE4.getSchema());

  @Test
  public void testAssignTablesByInstanceCount() {
    DeltaConfig config = DeltaConfig.builder()
      .setSource(new Stage("src", new Plugin("mysql", DeltaSource.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY)))
      .setTarget(new Stage("tgt", new Plugin("bq", DeltaTarget.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY)))
      .setParallelism(new ParallelismConfig(3))
      .setTables(Arrays.asList(TABLE1, TABLE2, TABLE3, TABLE4))
      .build();
    Map<Integer, Set<TableId>> expected = new HashMap<>();
    expected.put(0, new HashSet<>(Arrays.asList(TABLE1_ID, TABLE4_ID)));
    expected.put(1, Collections.singleton(TABLE2_ID));
    expected.put(2, Collections.singleton(TABLE3_ID));
    Assert.assertEquals(expected, DeltaWorker.assignTables(config));
  }

  @Test
  public void testAssignTablesFewerTablesThanInstances() {
    DeltaConfig config = DeltaConfig.builder()
      .setSource(new Stage("src", new Plugin("mysql", DeltaSource.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY)))
      .setTarget(new Stage("tgt", new Plugin("bq", DeltaTarget.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY)))
      .setParallelism(new ParallelismConfig(3))
      .setTables(Arrays.asList(TABLE1, TABLE2))
      .build();
    Map<Integer, Set<TableId>> expected = new HashMap<>();
    expected.put(0, Collections.singleton(TABLE1_ID));
    expected.put(1, Collections.singleton(TABLE2_ID));
    Assert.assertEquals(expected, DeltaWorker.assignTables(config));
  }

  @Test
  public void testAssignTablesDirectly() {
    Set<TableId> instance0Tables = new HashSet<>();
    instance0Tables.add(TABLE1_ID);
    instance0Tables.add(TABLE2_ID);
    instance0Tables.add(TABLE3_ID);
    Set<TableId> instance1Tables = new HashSet<>();
    instance1Tables.add(TABLE4_ID);

    DeltaConfig config = DeltaConfig.builder()
      .setSource(new Stage("src", new Plugin("mysql", DeltaSource.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY)))
      .setTarget(new Stage("tgt", new Plugin("bq", DeltaTarget.PLUGIN_TYPE, Collections.emptyMap(), Artifact.EMPTY)))
      .setParallelism(new ParallelismConfig(Arrays.asList(new InstanceConfig(instance0Tables),
                                                          new InstanceConfig(instance1Tables))))
      .setTables(Arrays.asList(TABLE1, TABLE2, TABLE3, TABLE4))
      .build();

    Map<Integer, Set<TableId>> expected = new HashMap<>();
    expected.put(0, instance0Tables);
    expected.put(1, instance1Tables);
    Map<Integer, Set<TableId>> assignments = DeltaWorker.assignTables(config);
    Assert.assertEquals(expected, assignments);
  }

  @Test
  public void testDeltaFailureRuntimeException() throws Exception {
    //Mock delta config
    DeltaConfig config = mock(DeltaConfig.class);
    when(config.getParallelism()).thenReturn(mock(ParallelismConfig.class));
    //Mock worker context,application specification,event reader and event consumer for initialize method
    WorkerContext workerContext = mock(WorkerContext.class);
    when(workerContext.getNamespace()).thenReturn("default");
    when(workerContext.getInstanceId()).thenReturn(0);
    when(workerContext.getRunId()).thenReturn(mock(RunId.class));
    DeltaSource deltaSource = mock(DeltaSource.class);
    DeltaTarget deltaTarget = mock(DeltaTarget.class);
    EventReader eventReader = mock(EventReader.class);
    EventConsumer eventConsumer = mock(EventConsumer.class);
    doNothing().when(eventReader).start(any());
    doNothing().when(eventConsumer).start();
    when(deltaSource.createReader(any(), any(), any())).thenReturn(eventReader);
    when(deltaTarget.createConsumer(any())).thenReturn(eventConsumer);
    when(workerContext.newPluginInstance(eq("Microsoft SQLServer"), any())).thenReturn(deltaSource);
    when(workerContext.newPluginInstance(eq("BigQuery"), any())).thenReturn(deltaTarget);
    ApplicationSpecification appSpec = PowerMockito.mock(DefaultApplicationSpecification.class);
    when(appSpec.getName()).thenReturn("DeltaWorker");
    when(workerContext.getApplicationSpecification()).thenReturn(appSpec);
    when(appSpec.getConfiguration()).thenReturn(
      new String(Files.readAllBytes(Paths.get("./src/test/java/io/cdap/delta/app/appSpec-config.json"))));

    WorkerSpecification spec = PowerMockito.mock(WorkerSpecification.class);
    PowerMockito.when(workerContext.getSpecification()).thenReturn(spec);
    PowerMockito.when(spec.getProperty("generation")).thenReturn("0");
    PowerMockito.when(spec.getProperty("table.assignments"))
      .thenReturn("{\"0\":[{\"database\":\"testreplication\",\"table\":\"npe\",\"schema\":\"dbo\"}]}");

    RemoteStateStore remoteStateStore = PowerMockito.mock(RemoteStateStore.class);
    PowerMockito.whenNew(RemoteStateStore.class).withArguments(workerContext).thenReturn(remoteStateStore);
    when(remoteStateStore.readState(any(), any())).thenReturn(null);

    StateStoreMigrator stateStoreMigrator = PowerMockito.mock(StateStoreMigrator.class);
    PowerMockito.whenNew(StateStoreMigrator.class).withArguments(any(), any(), any()).thenReturn(stateStoreMigrator);
    when(stateStoreMigrator.perform()).thenReturn(false);

    //Throw Delta Failure runtime exception while polling event queue in run method
    CapacityBoundedEventQueue eventQueue = mock(CapacityBoundedEventQueue.class);
    PowerMockito.whenNew(CapacityBoundedEventQueue.class).withArguments(anyInt(), anyLong()).thenReturn(eventQueue);
    doThrow(new DeltaFailureRuntimeException("")).when(eventQueue).poll(anyLong(), eq(TimeUnit.SECONDS));

    DeltaWorker deltaWorker = new DeltaWorker(config, null, mock(DefaultAppConfigurer.class));
    deltaWorker.configure(mock(WorkerConfigurer.class));
    deltaWorker.initialize(workerContext);
    exception.expect(DeltaFailureRuntimeException.class);
    deltaWorker.run();
  }
}
