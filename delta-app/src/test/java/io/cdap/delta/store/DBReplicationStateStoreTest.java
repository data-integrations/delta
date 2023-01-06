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
import io.cdap.delta.app.DeltaWorkerId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

public class DBReplicationStateStoreTest extends SystemAppTestBase {

  @Before
  public void setupTest() throws Exception {
    getStructuredTableAdmin().create(DBReplicationStateStore.TABLE_SPEC);
    setupRecords();
  }

  @After
  public void cleanupTest() throws Exception {
    getStructuredTableAdmin().drop(DBReplicationStateStore.TABLE_SPEC.getTableId());
  }

  @Test
  public void testGetNothing() throws TransactionException {
    getTransactionRunner().run(context -> {
      DBReplicationStateStore store = DBReplicationStateStore.get(context);

      byte[] stateResult = store.getStateData(getDeltaWorkerId("namespace", "appName", 123L, 0), "mystate");
      Assert.assertArrayEquals(new byte[0], stateResult);
    });
  }

  @Test
  public void testGetLatestGeneration() throws TransactionException {
    getTransactionRunner().run(context -> {
      DBReplicationStateStore store = DBReplicationStateStore.get(context);

      long notFoundResult = store.getLatestGeneration("namespace", "app_donot_exist");
      Assert.assertEquals(-1L, notFoundResult);

      long normalCaseResult = store.getLatestGeneration("namespace2", "appName1");
      Assert.assertEquals(100L, normalCaseResult);

      long higherResult = store.getLatestGeneration("namespace1", "appName1");
      Assert.assertEquals(101L, higherResult);
    });
  }

  @Test
  public void getWorkerInstances() throws TransactionException {
    getTransactionRunner().run(context -> {
      DBReplicationStateStore store = DBReplicationStateStore.get(context);

      //App doesn't exist
      Collection<Integer> emptyCollection = store.getWorkerInstances("namespace", "app_donot_exist", 100L);
      Assert.assertTrue(emptyCollection.isEmpty());

      //Generation doesn't exist
      Collection<Integer> emptyCollection2 = store.getWorkerInstances("namespace1", "appName1", 99L);
      Assert.assertTrue(emptyCollection2.isEmpty());

      //Normal Cases
      Collection<Integer> collection101 = store.getWorkerInstances("namespace1", "appName1", 101L);
      Assert.assertEquals(3, collection101.size());
      //Since it's not guaranteed to be sorted
      Assert.assertTrue(collection101.containsAll(Arrays.asList(0, 1, 2)));

      Collection<Integer> collection100 = store.getWorkerInstances("namespace2", "appName1", 100L);
      Assert.assertEquals(2, collection100.size());
      Assert.assertTrue(collection100.containsAll(Arrays.asList(0, 1)));
    });
  }

  @Test
  public void testCRUD() throws TransactionException {
    getTransactionRunner().run(context -> {
      DBReplicationStateStore store = DBReplicationStateStore.get(context);
      //Read
      byte[] result1 = store.getStateData(getDeltaWorkerId("namespace1", "appName1", 101L, 2), "mystate");
      Assert.assertArrayEquals("test data12".getBytes(StandardCharsets.UTF_8), result1);

      byte[] result2 = store.getStateData(getDeltaWorkerId("namespace1", "appName1", 101L, 2), "mystate3");
      Assert.assertArrayEquals("test data123".getBytes(StandardCharsets.UTF_8), result2);

      byte[] result3 = store.getStateData(getDeltaWorkerId("namespace2", "appName1", 100L, 1), "mystate");
      Assert.assertArrayEquals("test data201".getBytes(StandardCharsets.UTF_8), result3);

      // UPDATE OPERATIONS
      store.writeStateData(getDeltaWorkerId("namespace2", "appName1", 100L, 1), "mystate",
                           "test data201_update".getBytes(StandardCharsets.UTF_8));
      byte[] result4 = store.getStateData(getDeltaWorkerId("namespace2", "appName1", 100L, 1), "mystate");
      Assert.assertArrayEquals("test data201_update".getBytes(StandardCharsets.UTF_8), result4);

      //Others are not affected
      byte[] result5 = store.getStateData(getDeltaWorkerId("namespace2", "appName1", 100L, 0), "mystate");
      Assert.assertArrayEquals("test data200".getBytes(StandardCharsets.UTF_8), result5);
    });
  }

  private void setupRecords() throws TransactionException {
    getTransactionRunner().run(context -> {
      DBReplicationStateStore store = DBReplicationStateStore.get(context);

      store.writeStateData(getDeltaWorkerId("namespace1", "appName1", 100L, 0), "mystate",
                           "test data00".getBytes(StandardCharsets.UTF_8));
      store.writeStateData(getDeltaWorkerId("namespace1", "appName1", 100L, 1), "mystate",
                           "test data01".getBytes(StandardCharsets.UTF_8));

      store.writeStateData(getDeltaWorkerId("namespace1", "appName1", 101L, 0), "mystate",
                           "test data10".getBytes(StandardCharsets.UTF_8));
      store.writeStateData(getDeltaWorkerId("namespace1", "appName1", 101L, 0), "mystate2",
                           "test data102".getBytes(StandardCharsets.UTF_8));
      store.writeStateData(getDeltaWorkerId("namespace1", "appName1", 101L, 1), "mystate",
                           "test data11".getBytes(StandardCharsets.UTF_8));
      store.writeStateData(getDeltaWorkerId("namespace1", "appName1", 101L, 2), "mystate",
                           "test data12".getBytes(StandardCharsets.UTF_8));
      store.writeStateData(getDeltaWorkerId("namespace1", "appName1", 101L, 2), "mystate3",
                           "test data123".getBytes(StandardCharsets.UTF_8));

      store.writeStateData(getDeltaWorkerId("namespace2", "appName1", 100L, 0), "mystate",
                           "test data200".getBytes(StandardCharsets.UTF_8));
      store.writeStateData(getDeltaWorkerId("namespace2", "appName1", 100L, 1), "mystate",
                           "test data201".getBytes(StandardCharsets.UTF_8));
    });
  }

  private DeltaWorkerId getDeltaWorkerId(String namespace, String app, long generation, int instanceId) {
    return  new DeltaWorkerId(new DeltaPipelineId(namespace, app, generation), instanceId);
  }
}
