/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.delta.app.metrics;

import io.cdap.delta.api.DMLOperation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

public class EventCountsTest {

  private EventCounts eventCounts;

  @Before
  public void setup() {
    eventCounts = new EventCounts();
  }

  @Test
  public void testInitialValues() {
    Assert.assertEquals(0, eventCounts.getDdlEventCount());
    eventCounts.getDmlEventCounts()
      .forEach((type, count) -> Assert.assertEquals(0, count.get()));
  }

  @Test
  public void testIncrementAndClear() {
    eventCounts.incrementDMLCount(DMLOperation.Type.INSERT, 2);
    eventCounts.incrementDMLCount(DMLOperation.Type.DELETE);
    eventCounts.incrementDDLCount();

    Assert.assertEquals(1, eventCounts.getDdlEventCount());
    Assert.assertEquals(2, eventCounts.getDMLCount(DMLOperation.Type.INSERT));
    Assert.assertEquals(1, eventCounts.getDMLCount(DMLOperation.Type.DELETE));
    Assert.assertEquals(0, eventCounts.getDMLCount(DMLOperation.Type.UPDATE));

    eventCounts.clear();

    Assert.assertEquals(0, eventCounts.getDdlEventCount());
    eventCounts.getDmlEventCounts()
      .forEach((type, count) -> Assert.assertEquals(0, count.get()));
  }

  @Test
  public void testIncrementDMLMultiThreaded() throws InterruptedException {
    ExecutorService executorService = Executors.newFixedThreadPool(20);
    Collection<Callable<Void>> tasks = new ArrayList<>();

    IntStream.range(1, 20)
      .forEach((i) ->
                 tasks.add(() -> {
                   eventCounts.incrementDMLCount(DMLOperation.Type.INSERT);
                   eventCounts.incrementDMLCount(DMLOperation.Type.DELETE, 2);
                   return null;
                 }));

    tasks.add(() -> {
      eventCounts.incrementDMLCount(DMLOperation.Type.INSERT);
      eventCounts.incrementDMLCount(DMLOperation.Type.DELETE, 2);
      return null;
    });

    executorService.invokeAll(tasks);

    Assert.assertEquals(20, eventCounts.getDMLCount(DMLOperation.Type.INSERT));
    Assert.assertEquals(40, eventCounts.getDMLCount(DMLOperation.Type.DELETE));
    Assert.assertEquals(0, eventCounts.getDMLCount(DMLOperation.Type.UPDATE));
  }
}
