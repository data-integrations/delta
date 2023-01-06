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

import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.test.SystemAppTestBase;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.delta.api.DeltaPipelineId;
import io.cdap.delta.api.Offset;
import io.cdap.delta.app.DeltaWorkerId;
import io.cdap.delta.app.OffsetAndSequence;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DBStateStoreServiceTest extends SystemAppTestBase {

  @Before
  public void setupTest() throws Exception {
    getStructuredTableAdmin().create(DBReplicationOffsetStore.TABLE_SPEC);
    getStructuredTableAdmin().create(DBReplicationStateStore.TABLE_SPEC);
  }

  @After
  public void cleanupTest() throws Exception {
    getStructuredTableAdmin().drop(DBReplicationOffsetStore.TABLE_SPEC.getTableId());
    getStructuredTableAdmin().drop(DBReplicationStateStore.TABLE_SPEC.getTableId());
  }

  @Test
  public void offsetStoreCRUDTest() {
    DBStateStoreService service = new DBStateStoreService(getTransactionRunner());
    //INSERT
    //Record 1
    DeltaWorkerId id0 = new DeltaWorkerId(new DeltaPipelineId("namespace1", "appName1", 100L), 0);
    Map<String, String> offsetState = new HashMap<>();
    offsetState.put("offset0", "test_me_lsn0");
    OffsetAndSequence offset0 = new OffsetAndSequence(new Offset(offsetState), 0);
    service.writeOffset(id0, offset0);
    //Record 2
    DeltaWorkerId id1 = new DeltaWorkerId(new DeltaPipelineId("namespace1", "appName1", 100L), 1);
    Map<String, String> offsetState1 = new HashMap<>();
    offsetState1.put("offset1", "test_me_lsn1");
    OffsetAndSequence offset1 = new OffsetAndSequence(new Offset(offsetState), 1);
    service.writeOffset(id1, offset1);

    //READ
    OffsetAndSequence result0 = service.readOffset(id0);
    Assert.assertEquals(offset0, result0);

    OffsetAndSequence result1 = service.readOffset(id1);
    Assert.assertEquals(offset1, result1);

    //UPDATE
    offsetState.put("offset0", "test_me_lsn0_updated");
    offset0 = new OffsetAndSequence(new Offset(offsetState), 0);
    service.writeOffset(id0, offset0);

    OffsetAndSequence resultUpdated = service.readOffset(id0);
    Assert.assertEquals(offset0, resultUpdated);
  }

  @Test
  public void stateStoreCRUDTest() {
    DBStateStoreService service = new DBStateStoreService(getTransactionRunner());

    //INSERT

    DeltaWorkerId id1 = new DeltaWorkerId(new DeltaPipelineId("namespace1", "appName1", 100L), 0);
    service.writeState(id1, "state1", "stateData1".getBytes(StandardCharsets.UTF_8));
    service.writeState(id1, "state2", "stateData2".getBytes(StandardCharsets.UTF_8));

    //READ
    byte[] result1 = service.readState(id1 , "state1");
    Assert.assertArrayEquals("stateData1".getBytes(StandardCharsets.UTF_8), result1);

    byte[] result2 = service.readState(id1 , "state2");
    Assert.assertArrayEquals("stateData2".getBytes(StandardCharsets.UTF_8), result2);

    //UPDATE
    service.writeState(id1, "state1", "stateData1_updated".getBytes(StandardCharsets.UTF_8));
    byte[] result1Updated = service.readState(id1 , "state1");
    Assert.assertArrayEquals("stateData1_updated".getBytes(StandardCharsets.UTF_8), result1Updated);
  }

  @Test
  public void testGetLatestGeneration() throws TransactionException {
    String namespace = "latest_gen_namespace";
    setupRecords(namespace);
    DBStateStoreService service = new DBStateStoreService(getTransactionRunner());

    long result101 = service.getLatestGeneration(namespace, "appName1");
    Assert.assertEquals(101L, result101);
  }

  @Test
  public void testGetWorkerInstances() throws TransactionException {
    String namespace = "worker_instance_namespace";
    setupRecords(namespace);
    DBStateStoreService service = new DBStateStoreService(getTransactionRunner());

    //App doesn't exist
    Collection<Integer> emptyCollection = service
      .getWorkerInstances(new DeltaPipelineId(namespace, "app_donot_exist", 100L));
    Assert.assertTrue(emptyCollection.isEmpty());

    Collection<Integer> collection100 = service
      .getWorkerInstances(new DeltaPipelineId(namespace, "appName1", 101L));
    Assert.assertEquals(2, collection100.size());
    Assert.assertTrue(collection100.containsAll(Arrays.asList(0, 1)));
  }


  private void setupRecords(String namespace) throws TransactionException {
    getTransactionRunner().run(context -> {
      DBReplicationStateStore stateStore = DBReplicationStateStore.get(context);

      stateStore.writeStateData(getDeltaWorkerId(namespace, "appName1", 100L, 0), "mystate",
                                "test data00".getBytes(StandardCharsets.UTF_8));
      stateStore.writeStateData(getDeltaWorkerId(namespace, "appName1", 101L, 0), "mystate",
                                "test data10".getBytes(StandardCharsets.UTF_8));
      stateStore.writeStateData(getDeltaWorkerId(namespace, "appName1", 101L, 0), "mystate2",
                                "test data102".getBytes(StandardCharsets.UTF_8));
      stateStore.writeStateData(getDeltaWorkerId(namespace, "appName1", 101L, 1), "mystate",
                                "test data11".getBytes(StandardCharsets.UTF_8));
    });
  }

  private DeltaWorkerId getDeltaWorkerId(String namespace, String app, long generation, int instanceId) {
    return  new DeltaWorkerId(new DeltaPipelineId(namespace, app, generation), instanceId);
  }
}
