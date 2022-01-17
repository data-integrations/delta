/*
 * Copyright © 2021 Cask Data, Inc.
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

import io.cdap.delta.api.DeltaPipelineId;
import io.cdap.delta.api.Offset;
import io.cdap.delta.app.DeltaWorkerId;
import io.cdap.delta.app.OffsetAndSequence;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class StateStoreMigratorTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testEmptyHcfsPath() throws IOException {
    StateStoreMigrator stateStoreMigrator = new StateStoreMigrator("", null, null);
    stateStoreMigrator.perform();
    Assert.assertTrue(stateStoreMigrator.hcfsStateStore == null);

    stateStoreMigrator = new StateStoreMigrator(null, null, null);
    stateStoreMigrator.perform();
    Assert.assertTrue(stateStoreMigrator.hcfsStateStore == null);
  }

  @Test
  public void testNoGenerationPresent() throws IOException {
    File outputFolder = TMP_FOLDER.newFolder("no_generation_test", "checkpoint", "testdelta",
                                             "test_namespace", "test_app");
    String basePath = outputFolder.getAbsolutePath()
      .substring(0, outputFolder.getAbsolutePath().indexOf("/test_namespace"));

    DeltaWorkerId id = new DeltaWorkerId(new DeltaPipelineId("test_namespace", "test_app", 123), 0);

    StateStoreMigrator stateStoreMigrator = new StateStoreMigrator(basePath, id, null);
    stateStoreMigrator.perform();

    Assert.assertTrue(stateStoreMigrator.previousId == null);
  }

  @Test
  public void testGenerationPresentButNoMatchingInstance() throws IOException {
    File outputFolder = TMP_FOLDER.newFolder("mismatching_instance", "checkpoint", "testdelta",
                                             "test_namespace", "test_app", "123", "0");
    String basePath = outputFolder.getAbsolutePath()
      .substring(0, outputFolder.getAbsolutePath().indexOf("/test_namespace"));

    DeltaWorkerId id = new DeltaWorkerId(new DeltaPipelineId("test_namespace", "test_app", 125), 1);

    StateStoreMigrator stateStoreMigrator = new StateStoreMigrator(basePath, id, null);
    stateStoreMigrator.perform();

    //Should be able to find max generation
    DeltaWorkerId expectedPreviousId = new DeltaWorkerId(new DeltaPipelineId("test_namespace", "test_app", 123), 1);
    Assert.assertEquals(stateStoreMigrator.previousId, expectedPreviousId);

    //No migration should happen i.e no rename
    Assert.assertTrue(outputFolder.exists());
  }

  @Test
  public void testMatchingInstanceButNoData() throws IOException {
    File outputFolder = TMP_FOLDER.newFolder("matching_instance", "checkpoint", "testdelta",
                                             "test_namespace", "test_app", "123", "0");
    String basePath = outputFolder.getAbsolutePath()
      .substring(0, outputFolder.getAbsolutePath().indexOf("/test_namespace"));

    DeltaWorkerId id = new DeltaWorkerId(new DeltaPipelineId("test_namespace", "test_app", 125), 0);

    StateStoreMigrator stateStoreMigrator = new StateStoreMigrator(basePath, id, null);
    stateStoreMigrator.perform();

    //No migration should happen i.e no rename
    Assert.assertTrue(outputFolder.exists());
  }

  @Test
  public void testMigrateData() throws IOException {
    File basePath = TMP_FOLDER.newFolder("multi_instance_migration", "checkpoint", "testdelta");

    HCFSStateStore hcfsStateStore = HCFSStateStore.from(new Path(basePath.getAbsolutePath()));

    //Write some data for instance 0
    DeltaWorkerId previousId0 = new DeltaWorkerId(new DeltaPipelineId("test_namespace", "test_app", 123), 0);
    Map<String, String> offsetState = new HashMap<>();
    offsetState.put("offset0", "test_me_lsn0");
    OffsetAndSequence offset0 = new OffsetAndSequence(new Offset(offsetState), 0);
    hcfsStateStore.writeOffset(previousId0, offset0);
    byte[] pipelineData0 = "test_pipeline_state_data".getBytes(StandardCharsets.UTF_8);
    hcfsStateStore.writeState(previousId0, "pipeline", pipelineData0);
    byte[] stateHistoryData = "test_hist_state_data".getBytes(StandardCharsets.UTF_8);
    hcfsStateStore.writeState(previousId0, "state-history", stateHistoryData);

    //Write some data for instance 1
    DeltaWorkerId previousId1 = new DeltaWorkerId(new DeltaPipelineId("test_namespace", "test_app", 123), 1);
    Map<String, String> offsetState1 = new HashMap<>();
    offsetState.put("offset1", "test_me_lsn1");
    OffsetAndSequence offset1 = new OffsetAndSequence(new Offset(offsetState1), 1);
    hcfsStateStore.writeOffset(previousId1, offset1);
    byte[] pipelineData1 = "test_hist_state_data1".getBytes(StandardCharsets.UTF_8);
    hcfsStateStore.writeState(previousId1, "pipeline", pipelineData1);

    MockStateStore mockStateStore = new MockStateStore();

    //Migrate for instance 0
    DeltaWorkerId id0 = new DeltaWorkerId(new DeltaPipelineId("test_namespace", "test_app", 125), 0);
    StateStoreMigrator stateStoreMigrator = new StateStoreMigrator(basePath.toString(), id0, mockStateStore);
    stateStoreMigrator.perform();

    //Migrate for instance 1
    DeltaWorkerId id1 = new DeltaWorkerId(new DeltaPipelineId("test_namespace", "test_app", 125), 1);
    StateStoreMigrator stateStoreMigrator1 = new StateStoreMigrator(basePath.toString(), id1, mockStateStore);
    stateStoreMigrator1.perform();

    // Compare data in mocked state store
    //instance 0
    Assert.assertEquals(offset0, mockStateStore.readOffset(id0));
    Assert.assertArrayEquals(pipelineData0, mockStateStore.readState(id0, "pipeline"));
    Assert.assertArrayEquals(stateHistoryData, mockStateStore.readState(id0, "state-history"));
    //instance 1
    Assert.assertEquals(offset1, mockStateStore.readOffset(id1));
    Assert.assertArrayEquals(pipelineData1, mockStateStore.readState(id1, "pipeline"));

    //migration should rename
    Assert.assertTrue(getInstanceDirFromId(basePath, previousId0, "_migrated").exists());
    Assert.assertTrue(getInstanceDirFromId(basePath, previousId1, "_migrated").exists());
    Assert.assertTrue(!getInstanceDirFromId(basePath, previousId0, "").exists());
    Assert.assertTrue(!getInstanceDirFromId(basePath, previousId1, "").exists());
  }

  @Test
  public void testMigrateOnlyNonExistingDataOnDB() throws IOException {
    File basePath = TMP_FOLDER.newFolder("partial_migration", "checkpoint", "testdelta");

    HCFSStateStore hcfsStateStore = HCFSStateStore.from(new Path(basePath.getAbsolutePath()));

    //Write some data for instance 0
    DeltaWorkerId previousId0 = new DeltaWorkerId(new DeltaPipelineId("test_namespace", "test_app", 123), 0);
    Map<String, String> offsetState = new HashMap<>();
    offsetState.put("offset0", "test_me_lsn0");
    OffsetAndSequence offset0 = new OffsetAndSequence(new Offset(offsetState), 0);
    hcfsStateStore.writeOffset(previousId0, offset0);
    byte[] pipelineData0 = "test_pipeline_state_data".getBytes(StandardCharsets.UTF_8);
    hcfsStateStore.writeState(previousId0, "pipeline", pipelineData0);
    byte[] stateHistoryData = "test_hist_state_data".getBytes(StandardCharsets.UTF_8);
    hcfsStateStore.writeState(previousId0, "state-history", stateHistoryData);

    MockStateStore mockStateStore = new MockStateStore();
    //Insert 1 state ( simulates the situation if migration fails in between and only some of the states were migrated)
    DeltaWorkerId id0 = new DeltaWorkerId(new DeltaPipelineId("test_namespace", "test_app", 125), 0);
    byte[] existingPipelineData = "test_pipeline_state_data_exists".getBytes(StandardCharsets.UTF_8);
    mockStateStore.writeState(id0, "pipeline", existingPipelineData);

    //Migrate for instance 0
    StateStoreMigrator stateStoreMigrator = new StateStoreMigrator(basePath.toString(), id0, mockStateStore);
    stateStoreMigrator.perform();

    // Compare data in mocked state store
    //instance 0
    Assert.assertEquals(offset0, mockStateStore.readOffset(id0));
    Assert.assertArrayEquals(stateHistoryData, mockStateStore.readState(id0, "state-history"));
    //Existing data should not be over written
    Assert.assertArrayEquals(existingPipelineData, mockStateStore.readState(id0, "pipeline"));
    //migration should rename
    Assert.assertTrue(getInstanceDirFromId(basePath, previousId0, "_migrated").exists());
    Assert.assertTrue(!getInstanceDirFromId(basePath, previousId0, "").exists());
  }

  private File getInstanceDirFromId(File basePath, DeltaWorkerId id, String suffix) {
    return new File(basePath.getAbsolutePath() + String.format("/%s/%s/%d/%s",
                                                                    id.getPipelineId().getNamespace(),
                                                                    id.getPipelineId().getApp(),
                                                                    id.getPipelineId().getGeneration(),
                                                                    id.getInstanceId() + suffix));
  }
}
