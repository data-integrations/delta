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

import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.worker.AbstractWorker;
import io.cdap.cdap.api.worker.WorkerContext;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaTarget;
import io.cdap.delta.api.EventConsumer;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.Offset;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Worker implementation of a Delta pipeline.
 */
public class DeltaWorker extends AbstractWorker {
  private static final String SOURCE = "source";
  private static final String TARGET = "target";
  private static final String GENERATION = "generation";
  private static final String OFFSET_BASE_PATH = "offset.base.path";
  private CountDownLatch stopLatch;
  private Metrics metrics;
  private String sourceName;
  private String targetName;
  private String offsetBasePath;
  private DeltaContext deltaContext;
  private EventConsumer eventConsumer;
  private EventReader eventReader;
  private DeltaSource source;
  private DeltaTarget target;

  public DeltaWorker(String sourceName, String targetName, String offsetBasePath) {
    this.sourceName = sourceName;
    this.targetName = targetName;
    this.offsetBasePath = offsetBasePath;
  }

  @Override
  protected void configure() {
    Map<String, String> props = new HashMap<>();
    props.put(SOURCE, sourceName);
    props.put(TARGET, targetName);
    props.put(GENERATION, String.valueOf(System.currentTimeMillis()));
    props.put(OFFSET_BASE_PATH, offsetBasePath);
    setProperties(props);
  }

  @Override
  public void initialize(WorkerContext context) throws Exception {
    super.initialize(context);
    stopLatch = new CountDownLatch(1);
    sourceName = context.getSpecification().getProperty(SOURCE);
    targetName = context.getSpecification().getProperty(TARGET);
    offsetBasePath = context.getSpecification().getProperty(OFFSET_BASE_PATH);
    DeltaPipelineId id = new DeltaPipelineId(context.getNamespace(), context.getApplicationSpecification().getName(),
                                             context.getSpecification().getProperty(GENERATION));

    FileSystem fs = FileSystem.get(new Configuration());
    Path path = new Path(offsetBasePath);
    StateStore stateStore = new StateStore(fs, path);
    deltaContext = new DeltaContext(id, context.getRunId().getId(), metrics, stateStore, context);
    source = context.newPluginInstance(sourceName);
    target = context.newPluginInstance(targetName);
    eventConsumer = target.createConsumer(deltaContext);
    // TODO: load sequence number from offset store
    eventReader = source.createReader(deltaContext, new DirectEventEmitter(eventConsumer, deltaContext,
                                                                           System.currentTimeMillis()));
  }

  @Override
  public void run() {
    Offset offset;
    try {
      offset = deltaContext.loadOffset();
    } catch (IOException e) {
      // TODO: retry
      throw new RuntimeException("Error loading initial offset.", e);
    }
    eventConsumer.start();
    eventReader.start(offset);
    try {
      stopLatch.await();
    } catch (InterruptedException e) {
      // ignore and return, this is just waiting to stop anyway
    }
  }

  @Override
  public void stop() {
    try {
      eventReader.stop();
      eventConsumer.stop();
    } catch (Exception e) {
      // ignore and try to stop consumer
    }

    try {
      eventConsumer.stop();
    } finally {
      stopLatch.countDown();
    }
  }
}
