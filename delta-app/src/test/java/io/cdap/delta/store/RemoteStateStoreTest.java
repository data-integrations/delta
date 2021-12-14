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
import io.cdap.delta.app.DeltaWorkerId;
import io.cdap.delta.app.OffsetAndSequence;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class RemoteStateStoreTest {

  MockAssessmentServiceClient mockAssessmentServiceClient;
  RemoteStateStore remoteStateStore;

  @Before
  public void setup() {
    mockAssessmentServiceClient = new MockAssessmentServiceClient(null);
    remoteStateStore = new RemoteStateStore(mockAssessmentServiceClient);
  }

  @Test
  public void testReadOffset() {
    DeltaWorkerId idNull = new DeltaWorkerId(new DeltaPipelineId("namespace", "app", 100L), 0);
    Assert.assertNull(remoteStateStore.readOffset(idNull));

    DeltaWorkerId idMatch = new DeltaWorkerId(new DeltaPipelineId("namespace", "app", 110L), 0);
    OffsetAndSequence actualOffset = remoteStateStore.readOffset(idMatch);
    Assert.assertEquals(mockAssessmentServiceClient.getOffsetMock().getSequenceNumber(),
                        actualOffset.getSequenceNumber());
    Assert.assertEquals(mockAssessmentServiceClient.getOffsetMock().getOffset().get().get("offset1"),
                        actualOffset.getOffset().get().get("offset1"));
  }

  @Test
  public void testWriteOffset() {
    DeltaWorkerId id = new DeltaWorkerId(new DeltaPipelineId("namespace", "app", 100L), 0);
    //Will throw an Exception if the request body doesn't match.
    remoteStateStore.writeOffset(id, mockAssessmentServiceClient.getOffsetMock());
  }

  @Test
  public void testReadState() {
    DeltaWorkerId id = new DeltaWorkerId(new DeltaPipelineId("namespace", "app", 100L), 0);
    byte[] byteArray = mockAssessmentServiceClient.getStateDataMock().getBytes(StandardCharsets.UTF_8);
    Assert.assertArrayEquals(byteArray, remoteStateStore.readState(id, "test_state"));
  }

  @Test
  public void testWriteState() {
    DeltaWorkerId id = new DeltaWorkerId(new DeltaPipelineId("namespace", "app", 100L), 0);
    byte[] byteArray = mockAssessmentServiceClient.getStateDataMock().getBytes(StandardCharsets.UTF_8);
    //Will throw an Exception if the request body doesn't match.
    remoteStateStore.writeState(id, "test_state", byteArray);
  }

}
