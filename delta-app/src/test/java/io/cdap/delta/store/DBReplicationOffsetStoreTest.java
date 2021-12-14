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

import com.google.gson.Gson;
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

import java.util.HashMap;
import java.util.Map;

public class DBReplicationOffsetStoreTest extends SystemAppTestBase {

  private static final Gson GSON = new Gson();

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  @Before
  public void setupTest() throws Exception {
    getStructuredTableAdmin().create(DBReplicationOffsetStore.TABLE_SPEC);
  }

  @After
  public void cleanupTest() throws Exception {
    getStructuredTableAdmin().drop(DBReplicationOffsetStore.TABLE_SPEC.getTableId());
  }

  @Test
  public void testGetNothing() throws TransactionException {
    getTransactionRunner().run(context -> {
      DBReplicationOffsetStore store = DBReplicationOffsetStore.get(context);

      OffsetAndSequence offsetResult = store.getOffsets(getDeltaWorkerId("namespace", "appName", 123L, 0));
      Assert.assertNull(offsetResult);
    });
  }

  @Test
  public void testCRUD() throws TransactionException {
    //Create 3 offset objects.
    Map<String, String> offsetState = new HashMap<>();

    offsetState.put("offset1", "test_me_lsn1");
    OffsetAndSequence offset1 = new OffsetAndSequence(new Offset(offsetState), 1);

    offsetState.clear();
    offsetState.put("offset2", "test_me_lsn2");
    OffsetAndSequence offset2 = new OffsetAndSequence(new Offset(offsetState), 2);

    offsetState.clear();
    offsetState.put("offset3", "test_me_lsn3");
    OffsetAndSequence offset3 = new OffsetAndSequence(new Offset(offsetState), 3);

    getTransactionRunner().run(context -> {
      DBReplicationOffsetStore store = DBReplicationOffsetStore.get(context);

      //Insert 3 events
      store.writeOffset(getDeltaWorkerId("namespace1", "appName1", 100L, 0), offset1);
      store.writeOffset(getDeltaWorkerId("namespace1", "appName1", 100L, 1), offset2);
      store.writeOffset(getDeltaWorkerId("namespace2", "appName", 100L, 1), offset3);

      //Read and assert these 4 events
      OffsetAndSequence result1 = store.getOffsets(getDeltaWorkerId("namespace1", "appName1", 100L, 0));
      Assert.assertEquals(offset1, result1);

      OffsetAndSequence result2 = store.getOffsets(getDeltaWorkerId("namespace1", "appName1", 100L, 1));
      Assert.assertEquals(offset2, result2);
      Assert.assertNotEquals(result2, result1);

      OffsetAndSequence result3 = store.getOffsets(getDeltaWorkerId("namespace2", "appName", 100L, 1));
      Assert.assertNotEquals(result3, result2);

      // UPDATE OPERATIONS
      offsetState.clear();
      offsetState.put("offset1", "test_me_lsn_updated");
      OffsetAndSequence offset1Updated = new OffsetAndSequence(new Offset(offsetState), 2);
      store.writeOffset(getDeltaWorkerId("namespace1", "appName1", 100L, 0), offset1Updated);

      OffsetAndSequence result1Updated = store.getOffsets(getDeltaWorkerId("namespace1", "appName1", 100L, 0));

      Assert.assertEquals(offset1Updated, result1Updated);
      Assert.assertNotEquals(result1, result1Updated);

      //Others are not affected
      result2 = store.getOffsets(getDeltaWorkerId("namespace1", "appName1", 100L, 1));
      Assert.assertEquals(offset2, result2);

    });
  }

  private DeltaWorkerId getDeltaWorkerId(String namespace, String app, long generation, int instanceId) {
    return  new DeltaWorkerId(new DeltaPipelineId(namespace, app, generation), instanceId);
  }
}
