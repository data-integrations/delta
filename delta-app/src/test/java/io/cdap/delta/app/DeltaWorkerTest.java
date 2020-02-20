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

import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaTarget;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.proto.Artifact;
import io.cdap.delta.proto.DeltaConfig;
import io.cdap.delta.proto.InstanceConfig;
import io.cdap.delta.proto.ParallelismConfig;
import io.cdap.delta.proto.Plugin;
import io.cdap.delta.proto.Stage;
import io.cdap.delta.proto.TableId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link DeltaWorker}.
 */
public class DeltaWorkerTest {
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
}
