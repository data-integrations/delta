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

import com.google.gson.Gson;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.worker.AbstractWorker;
import io.cdap.cdap.api.worker.WorkerContext;
import io.cdap.delta.api.ChangeEvent;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.DeltaFailureException;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaTarget;
import io.cdap.delta.api.EventConsumer;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.EventReaderDefinition;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.Sequenced;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.proto.DeltaConfig;
import io.cdap.delta.store.DefaultMacroEvaluator;
import io.cdap.delta.store.StateStore;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Worker implementation of a Delta pipeline.
 */
public class DeltaWorker extends AbstractWorker {
  public static final String NAME = "DeltaWorker";
  private static final Logger LOG = LoggerFactory.getLogger(DeltaWorker.class);
  private static final Gson GSON = new Gson();
  private static final String GENERATION = "generation";

  private final AtomicBoolean shouldStop = new AtomicBoolean(false);

  // this is injected by CDAP
  @SuppressWarnings("unused")
  private Metrics metrics;

  private DeltaContext deltaContext;
  private EventConsumer eventConsumer;
  private EventReader eventReader;
  private DeltaSource source;
  private DeltaTarget target;
  private EventReaderDefinition readerDefinition;
  private Offset offset;
  private BlockingQueue<Sequenced<? extends ChangeEvent>> eventQueue;
  private int maxRetrySeconds;
  private int retryDelaySeconds;

  @Override
  protected void configure() {
    setName(NAME);
    Map<String, String> props = new HashMap<>();
    // generation is used in cases where pipeline X is created, then deleted, then created again.
    // in those situations, we don't want to start from the offset that it had before it was deleted,
    // so we include the generation as part of the path when storing state.
    props.put(GENERATION, String.valueOf(System.currentTimeMillis()));
    setProperties(props);
  }

  @Override
  public void initialize(WorkerContext context) throws Exception {
    super.initialize(context);
    long generation = Long.parseLong(context.getSpecification().getProperty(GENERATION));

    ApplicationSpecification appSpec = context.getApplicationSpecification();
    DeltaConfig config = GSON.fromJson(appSpec.getConfiguration(), DeltaConfig.class);

    String sourceName = config.getSource().getName();
    String targetName = config.getTarget().getName();
    String offsetBasePath = config.getOffsetBasePath();
    maxRetrySeconds = config.getRetryConfig().getMaxDurationSeconds();
    retryDelaySeconds = config.getRetryConfig().getDelaySeconds();
    DeltaPipelineId id = new DeltaPipelineId(context.getNamespace(), appSpec.getName(), generation);

    FileSystem fs = FileSystem.get(new Configuration());
    Path path = new Path(offsetBasePath);
    StateStore stateStore = new StateStore(fs, path);
    EventMetrics eventMetrics = new EventMetrics(metrics, targetName);
    PipelineStateService stateService = new PipelineStateService(id, stateStore);
    stateService.load();
    deltaContext = new DeltaContext(id, context.getRunId().getId(), metrics, stateStore, context, eventMetrics,
                                    stateService);
    MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(context.getRuntimeArguments(),
                                                              context, context.getNamespace());
    source = context.newPluginInstance(sourceName, macroEvaluator);
    target = context.newPluginInstance(targetName, macroEvaluator);
    OffsetAndSequence offsetAndSequence = deltaContext.loadOffset();
    offset = offsetAndSequence.getOffset();

    Set<DDLOperation> ddlBlacklist = new HashSet<>(config.getDdlBlacklist());
    // targets will not behave properly if they don't get create table events
    ddlBlacklist.remove(DDLOperation.CREATE_TABLE);
    Set<SourceTable> expandedTables = config.getTables().stream()
      .map(t -> {
        Set<DMLOperation> expandedDmlBlacklist = new HashSet<>(t.getDmlBlacklist());
        expandedDmlBlacklist.addAll(config.getDmlBlacklist());
        Set<DDLOperation> expandedDdlBlacklist = new HashSet<>(t.getDdlBlacklist());
        expandedDdlBlacklist.addAll(ddlBlacklist);
        expandedDdlBlacklist.remove(DDLOperation.CREATE_TABLE);
        return new SourceTable(t.getDatabase(), t.getTable(), t.getSchema(),
                               t.getColumns(), expandedDmlBlacklist, expandedDdlBlacklist);
      }).collect(Collectors.toSet());
    readerDefinition = new EventReaderDefinition(expandedTables,
                                                 config.getDmlBlacklist(),
                                                 config.getDdlBlacklist());

    // TODO: make the queue size configurable? record metrics about queue size?
    eventQueue = new ArrayBlockingQueue<>(100);
  }

  @Override
  public void run() {
    try {
      if (offset.get().isEmpty()) {
        deltaContext.setOK();
      }
      startFromLastCommit();
    } catch (Exception e) {
      // if this fails at the start of the run, fail the entire run as it probably means there is something
      // wrong with the configuration or environment
      throw new RuntimeException(e.getMessage(), e);
    }

    AtomicReference<Throwable> error = new AtomicReference<>();
    // in most circumstances, the worker is stopped, which causes eventQueue.take() to be interrupted and break out
    // of the loop. However, it is also possible for the worker to be stopped before it gets here, in which case it
    // should just end immediately.
    while (!shouldStop.get()) {

      // create the retry policy for handling exceptions thrown from calls to applyEvent()
      // the worker will retry up to the configured maximum amount of time.
      // On each failed attempt, offset will be rolled back to the last commit, and any in-memory state
      // related to metrics and queued change events will be cleared away.
      RetryPolicy<Object> retryPolicy = new RetryPolicy<>()
        // max attempts defaults to 3 if it is not set. Set this to max value so that retries
        // are dictated by duration and not attempts
        .withMaxAttempts(Integer.MAX_VALUE)
        .withMaxDuration(Duration.of(maxRetrySeconds, ChronoUnit.SECONDS))
        .withDelay(retryDelaySeconds == 0 ? Duration.of(1, ChronoUnit.MILLIS) :
          Duration.of(retryDelaySeconds, ChronoUnit.SECONDS))
        .abortIf(o -> shouldStop.get())
        .onFailedAttempt(failureContext -> {
          Throwable failure = failureContext.getLastFailure();
          error.set(failure);
          if (failure instanceof DeltaFailureException) {
            LOG.warn("Encountered an error that cannot be retried. Failing the pipeline...", failure);
            shouldStop.set(true);
            return;
          }

          LOG.warn("Encountered an error while attempting to apply an event. "
                     + "Events will be replayed from the last successful commit.", failure);
          // if there was an error applying the event, stop the current reader and consumer
          // and restart from the last commit. We cannot just retry applying the single event because that would force
          // targets to persist their changes before they can return from applyDML or applyDDL.
          // For example, a consumer may want to stream events to the file system as they get them, and periodically
          // load batches of 100 events into the target storage system. If there is an error writing to the file system
          // for event 50, there is no way for the consumer to rewind and write events 1-49 again.
          try {
            eventConsumer.stop();
            eventReader.stop();
          } catch (InterruptedException ex) {
            // if stopping is interrupted, it means the worker is shutting down.
            // in this scenario, we want to break out of the retry loop instead
            // of trying to reset the state and keep reading
            shouldStop.set(true);
            return;
          }

          // If startFromLastCommit fails here, it is likely a recoverable issue, since a successful call was
          // already performed. So in this scenario, retry for some time limit before failing.
          // this can fail, for example, if the offset store is unavailable due to a temporary outage.
          Failsafe.with(new RetryPolicy<>()
                          .withBackoff(1, 120, ChronoUnit.SECONDS)
                          .withMaxAttempts(Integer.MAX_VALUE)
                          .withMaxDuration(Duration.of(60, ChronoUnit.MINUTES))
                          .onFailedAttempt(e1 -> {
                            // if it's the first failed attempt, or we have retried for longer than a minute,
                            // log the fact that we are failing to start from the last commit
                            if (e1.getAttemptCount() == 1 ||
                              Duration.of(1, ChronoUnit.MINUTES).minus(e1.getElapsedTime()).isNegative()) {
                              LOG.warn("Unable to reset state to the latest commit point.", e1.getLastFailure());
                            }
                          }))
            .run(this::startFromLastCommit);
        })
        .onRetriesExceeded(failureContext -> {
          long secondsElapsed = failureContext.getElapsedTime().get(ChronoUnit.SECONDS);
          if (secondsElapsed < 60) {
            LOG.error("Failures have been ongoing for {} seconds. Failing the program.", secondsElapsed);
          } else {
            LOG.error("Failures have been ongoing for {} minutes. Failing the program.",
                      failureContext.getElapsedTime().get(ChronoUnit.MINUTES));
          }
        });

      // take change events emitted by the source and tell the target to apply them, one by one.
      Failsafe.with(retryPolicy).run(() -> {
        Sequenced<? extends ChangeEvent> event;
        try {
          // set a 1 second limit on waiting for a new change event to allow the worker to
          // respond to a stop() call and break out of the run loop.
          event = eventQueue.poll(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          // happens if the worker is killed. Break out of the run loop and let it end.
          shouldStop.set(true);
          return;
        }

        // this is checked in case the worker was stopped in the middle of the eventQueue.poll() call
        if (shouldStop.get()) {
          return;
        }

        // this happens if the source hasn't emitted anything within the last second
        if (event == null) {
          return;
        }

        applyEvent(event);
        error.set(null);
      });
    }

    // if there was an error, throw it so that the program state goes to FAILED and not KILLED
    // to distinguish between pipelines that failed and pipelines that were stopped.
    if (error.get() != null) {
      throw new RuntimeException(error.get());
    }
  }

  @Override
  public void stop() {
    try {
      eventReader.stop();
    } catch (Exception e) {
      // ignore and try to stop consumer
    } finally {
      try {
        eventConsumer.stop();
      } catch (Exception e) {
        // ignore and proceed to exit
      } finally {
        shouldStop.set(true);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void applyEvent(Sequenced<? extends ChangeEvent> event) throws Exception {
    switch (event.getEvent().getChangeType()) {
      case DDL:
        eventConsumer.applyDDL((Sequenced<DDLEvent>) event);
        break;
      case DML:
        eventConsumer.applyDML((Sequenced<DMLEvent>) event);
        break;
      default:
        // this can only happen if there is a bug in the program
        LOG.error("Skipping unknown change type {}", event.getEvent().getChangeType());
    }
  }

  private void startFromLastCommit() throws Exception {
    eventQueue.clear();
    deltaContext.clearMetrics();

    OffsetAndSequence offsetAndSequence = deltaContext.loadOffset();
    offset = offsetAndSequence.getOffset();
    QueueingEventEmitter emitter = new QueueingEventEmitter(readerDefinition, offsetAndSequence.getSequenceNumber(),
                                                            eventQueue);

    eventReader = source.createReader(readerDefinition, deltaContext, emitter);
    eventConsumer = target.createConsumer(deltaContext);

    eventReader.start(offset);
    eventConsumer.start();
  }
}
