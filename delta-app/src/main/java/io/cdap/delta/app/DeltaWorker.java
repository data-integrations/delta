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

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.app.ApplicationConfigurer;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.app.RuntimeConfigurer;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.worker.AbstractWorker;
import io.cdap.cdap.api.worker.WorkerContext;
import io.cdap.cdap.api.worker.WorkerSpecification;
import io.cdap.delta.api.ChangeEvent;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.DeltaFailureException;
import io.cdap.delta.api.DeltaFailureRuntimeException;
import io.cdap.delta.api.DeltaPipelineId;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaTarget;
import io.cdap.delta.api.EventConsumer;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.EventReaderDefinition;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.Sequenced;
import io.cdap.delta.api.SourceProperties;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.api.StopContext;
import io.cdap.delta.proto.ColumnTransformation;
import io.cdap.delta.proto.DeltaConfig;
import io.cdap.delta.proto.InstanceConfig;
import io.cdap.delta.proto.ParallelismConfig;
import io.cdap.delta.proto.TableId;
import io.cdap.delta.proto.TableTransformation;
import io.cdap.delta.store.DefaultMacroEvaluator;
import io.cdap.delta.store.RemoteStateStore;
import io.cdap.delta.store.StateStore;
import io.cdap.delta.store.StateStoreMigrator;
import io.cdap.transformation.DefaultTransformationContext;
import io.cdap.transformation.TransformationUtil;
import io.cdap.transformation.api.Transformation;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Worker implementation of a Delta pipeline.
 */
public class DeltaWorker extends AbstractWorker {
  public static final String NAME = "DeltaWorker";
  private static final Logger LOG = LoggerFactory.getLogger(DeltaWorker.class);
  private static final Gson GSON = new Gson();
  private static final String GENERATION = "generation";
  private static final String TABLE_ASSIGNMENTS = "table.assignments";
  private static final String EVENT_QUEUE_SIZE = "event.queue.size";
  private static final String EVENT_QUEUE_CAPACITY = "event.queue.capacity";
  private static final String SOURCE_PROPERTIES = "source.properties";
  private static final Type TABLE_ASSIGNMENTS_TYPE = new TypeToken<Map<Integer, Set<TableId>>>() { }.getType();

  private final AtomicBoolean shouldStop;

  // this is injected by CDAP
  @SuppressWarnings("unused")
  private Metrics metrics;

  private DeltaConfig config;
  private SourceProperties sourceProperties;
  private ApplicationConfigurer applicationConfigurer;
  private DeltaContext deltaContext;
  private EventConsumer eventConsumer;
  private EventReader eventReader;
  private DeltaSource source;
  private DeltaTarget target;
  private EventReaderDefinition readerDefinition;
  private Offset offset;
  private BlockingQueue<Sequenced<? extends ChangeEvent>> eventQueue;
  private Map<Integer, Set<TableId>> tableAssignments;
  // a map whose key is the table name and value is a list of transformation that will be applied on this table in order
  private Map<String, List<Transformation>> transformations;
  private int maxRetrySeconds;
  private int retryDelaySeconds;
  private int eventQueueSize;
  private long eventQueueCapacity;

  // no-arg constructor required to initialize the shouldStop variable, since CDAP calls the no-arg constructor
  // and sets fields through reflection.
  DeltaWorker() {
    this.shouldStop = new AtomicBoolean(false);
  }

  DeltaWorker(DeltaConfig config, SourceProperties sourceProperties, ApplicationConfigurer applicationConfigurer) {
    this();
    this.config = config;
    this.sourceProperties = sourceProperties;
    this.applicationConfigurer = applicationConfigurer;
  }

  @Override
  protected void configure() {
    setName(NAME);
    Map<String, String> props = new HashMap<>();
    // generation is used in cases where pipeline X is created, then deleted, then created again.
    // in those situations, we don't want to start from the offset that it had before it was deleted,
    // so we include the generation as part of the path when storing state.
    props.put(GENERATION, String.valueOf(System.currentTimeMillis()));

    //Offset is dependent on generation (timestamp), this is stored once during initial deployment.
    //Use this from app spec during runtime as well as upgrade calls.
    if (applicationConfigurer != null && applicationConfigurer.getDeployedApplicationSpec() != null) {
      WorkerSpecification workerSpec = applicationConfigurer.getDeployedApplicationSpec().getWorkers().get(NAME);
      props.put(GENERATION, workerSpec.getProperties().get(GENERATION));
    }

    tableAssignments = assignTables(config);
    props.put(TABLE_ASSIGNMENTS, GSON.toJson(tableAssignments));
    if (sourceProperties != null) {
      props.put(SOURCE_PROPERTIES, GSON.toJson(sourceProperties));
    }
    Integer numInstances = config.getParallelism().getNumInstances();
    if (numInstances != null && numInstances > tableAssignments.size()) {
      LOG.warn("Due to the number of source tables, "
                 + "ignoring the configuration to use {} instances and using {} instead.",
               numInstances, tableAssignments.size());
    }
    // tableAssignments can be empty if no source tables are given
    // in that scenario, a single instance is used to read all tables
    setInstances(Math.max(1, tableAssignments.size()));
    setResources(new Resources(2048, 2));
    setProperties(props);
  }

  @Override
  public void initialize(WorkerContext context) throws Exception {
    super.initialize(context);
    long generation = Long.parseLong(context.getSpecification().getProperty(GENERATION));
    ApplicationSpecification appSpec = context.getApplicationSpecification();
    StateStore stateStore = new RemoteStateStore(context);
    DeltaWorkerId id = new DeltaWorkerId(new DeltaPipelineId(context.getNamespace(), appSpec.getName(), generation),
                                         context.getInstanceId());
    config = GSON.fromJson(appSpec.getConfiguration(), DeltaConfig.class);

    StateStoreMigrator stateStoreMigrator = new StateStoreMigrator(config.getOffsetBasePath(), id, stateStore);
    if (stateStoreMigrator.perform()) {
      LOG.debug("State store migration was performed successfully for the instance {} from hcfs location {}",
                id.getInstanceId(), config.getOffsetBasePath());
    }

    int instanceId = context.getInstanceId();
    tableAssignments = GSON.fromJson(context.getSpecification().getProperty(TABLE_ASSIGNMENTS), TABLE_ASSIGNMENTS_TYPE);
    Set<TableId> assignedTables = tableAssignments.getOrDefault(instanceId, new HashSet<>());
    if (assignedTables == null) {
      return;
    }

    WorkerSpecification workerSpec = context.getSpecification();
    String sourcePropertiesJSON = workerSpec.getProperties().getOrDefault(SOURCE_PROPERTIES, null);
    SourceProperties sourceProperties = sourcePropertiesJSON == null ? null :
      GSON.fromJson(sourcePropertiesJSON, SourceProperties.class);

    String sourceName = config.getSource().getName();
    String targetName = config.getTarget().getName();
    maxRetrySeconds = config.getRetryConfig().getMaxDurationSeconds();
    retryDelaySeconds = config.getRetryConfig().getDelaySeconds();

    PipelineStateService stateService = new PipelineStateService(id, stateStore);
    stateService.load();
    deltaContext = new DeltaContext(id, context.getRunId().getId(), metrics, stateStore, context,
                                    stateService, config.getRetryConfig().getMaxDurationSeconds(),
                                    context.getRuntimeArguments(), sourceProperties, config.getTables());
    MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(context.getRuntimeArguments(),
                                                              context, context.getNamespace());
    source = context.newPluginInstance(sourceName, macroEvaluator);
    source.initialize(deltaContext);
    target = context.newPluginInstance(targetName, macroEvaluator);
    target.initialize(deltaContext);
    OffsetAndSequence offsetAndSequence = deltaContext.loadOffset();
    offset = offsetAndSequence.getOffset();

    Set<DDLOperation.Type> ddlBlacklist = new HashSet<>(config.getDdlBlacklist());
    // targets will not behave properly if they don't get create table events
    ddlBlacklist.remove(DDLOperation.Type.CREATE_TABLE);
    Map<String, TableTransformation> tableTransformations =
      config.getTableTransformations().stream().collect(Collectors.toMap(TableTransformation::getTableName,
                                                                         Function.identity()));
    transformations = new HashMap<>();
    Set<SourceTable> expandedTables = config.getTables().stream()
      .filter(t -> assignedTables.contains(new TableId(t.getDatabase(), t.getTable(), t.getSchema())))
      .map(t -> {
        Set<DMLOperation.Type> expandedDmlBlacklist = new HashSet<>(t.getDmlBlacklist());
        expandedDmlBlacklist.addAll(config.getDmlBlacklist());
        Set<DDLOperation.Type> expandedDdlBlacklist = new HashSet<>(t.getDdlBlacklist());
        expandedDdlBlacklist.addAll(ddlBlacklist);
        expandedDdlBlacklist.remove(DDLOperation.Type.CREATE_TABLE);
        loadTransformations(context, t, tableTransformations);
        return new SourceTable(t.getDatabase(), t.getTable(), t.getSchema(),
                               t.getColumns(), expandedDmlBlacklist, expandedDdlBlacklist);
      })
      .collect(Collectors.toSet());
    readerDefinition = new EventReaderDefinition(expandedTables,
                                                 config.getDmlBlacklist(),
                                                 config.getDdlBlacklist());
    // Use a small queue size by default since we want to prioritize reliability (avoid OOM)
    eventQueueSize = Integer.parseInt(context.getRuntimeArguments().getOrDefault(EVENT_QUEUE_SIZE, "10"));
    eventQueueCapacity = Long.parseLong(context.getRuntimeArguments().getOrDefault(EVENT_QUEUE_CAPACITY, "10000000"));
    eventQueue = new CapacityBoundedEventQueue(eventQueueSize, eventQueueCapacity);
  }

  private void loadTransformations(WorkerContext context, SourceTable table,
                                   Map<String, TableTransformation> tableTransformations) {
    String tableName = table.getSchema() == null ? table.getTable() : table.getSchema() + "." + table.getTable();
    List<Transformation> columnTransformations = new ArrayList<>();
    transformations.put(tableName, columnTransformations);
    TableTransformation tableTransformation = tableTransformations.get(tableName);
    if (tableTransformation == null) {
      return;
    }
    int index = 0;
    for (ColumnTransformation columnTransformation : tableTransformation.getColumnTransformations()) {
      String directive = columnTransformation.getDirective();
        try {
          Transformation transformation = context.newPluginInstance(tableName + index++);
          transformation.initialize(new DefaultTransformationContext(directive));
          columnTransformations.add(transformation);
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to load transformation plugin for directive : %s. Error : " +
                                                     "%s.", directive, e.getMessage()), e);
        }
      };
  }

  @Override
  public void run() {
    // handle the situation where the user manually increased the number of worker instances after
    // the pipeline was deployed.
    int instanceId = getContext().getInstanceId();
    if (!config.getTables().isEmpty() && !tableAssignments.containsKey(instanceId)) {
      LOG.warn("Instance {} was not assigned any tables when the pipeline was created and will be shut down.",
               instanceId);
      return;
    }

    try {
      if (offset.get().isEmpty()) {
        deltaContext.setOK();
      }
      startFromLastCommit();
    } catch (RuntimeException e) {
      throw e;
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
        // For some reason, Failsafe treats
        // .abortIf((result, throwable)
        // differently than
        // .abortIf(o -> shouldStop.get())
        // the latter Predicate gets turned into a BiPredicate that returns false whenever result is null,
        // which is always null in this code. In other words, it will never abort
        .abortIf((result, throwable) -> shouldStop.get())
        .onFailedAttempt(failureContext -> {
          Throwable failure = failureContext.getLastFailure();
          error.set(failure);
          if (failure instanceof DeltaFailureException || failure instanceof DeltaFailureRuntimeException) {
            LOG.error("Encountered an error that cannot be retried. Failing the pipeline...", failure);
            shouldStop.set(true);
          }

          // can be set if the failure is a DeltaFailure, or if the worker was stopped
          if (shouldStop.get()) {
            return;
          }

          LOG.warn("Encountered an error. Events will be replayed from the last successful commit.", failure);
          // if there was an error applying the event, stop the current reader and consumer
          // and restart from the last commit. We cannot just retry applying the single event because that would force
          // targets to persist their changes before they can return from applyDML or applyDDL.
          // For example, a consumer may want to stream events to the file system as they get them, and periodically
          // load batches of 100 events into the target storage system. If there is an error writing to the file system
          // for event 50, there is no way for the consumer to rewind and write events 1-49 again.
          try {
            LOG.info("Stopping Event Reader...");
            eventReader.stop(new StopContext() {
              @Override
              public Origin getOrigin() {
                return Origin.ERROR;
              }
            });
            LOG.info("Stopped Event Reader.");
          } catch (InterruptedException ex) {
            // if stopping is interrupted, it means the worker is shutting down.
            // in this scenario, we want to break out of the retry loop instead
            // of trying to reset the state and keep reading
            shouldStop.set(true);
          } catch (Exception ex) {
            // if we couldn't stop the reader, bail out.
            LOG.error("Unable to stop event reader. The replicator will be stopped.", ex);
            shouldStop.set(true);
          }

          try {
            LOG.info("Stopping Event Consumer...");
            eventConsumer.stop();
            LOG.info("Stopped Event Consumer.");
          } catch (InterruptedException ex) {
            // if stopping is interrupted, it means the worker is shutting down.
            // in this scenario, we want to break out of the retry loop instead
            // of trying to reset the state and keep reading
            shouldStop.set(true);
          } catch (Exception ex) {
            // if we couldn't stop the consumer, bail out.
            LOG.error("Unable to stop event consumer. The replicator will be stopped.", ex);
            shouldStop.set(true);
          }

          if (shouldStop.get()) {
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

        try {
          deltaContext.throwFailureIfExists();
        } catch (Exception e) {
          throw e;
        } catch (Throwable throwable) {
          // if the failure wasn't an exception, fail right away since it should be some sort of
          // code issue and not something that can succeed on retry
          throw new DeltaFailureException(throwable.getMessage(), throwable);
        }

        // this is checked in case the worker was stopped in the middle of the eventQueue.poll() call
        if (shouldStop.get()) {
          return;
        }

        // this happens if the source hasn't emitted anything within the last second
        if (event == null) {
          return;
        }
        event = transformEvent(event);
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
    shouldStop.set(true);
    try {
      eventReader.stop(new StopContext() {
        @Override
        public Origin getOrigin() {
          return Origin.USER;
        }
      });
    } catch (Exception e) {
      // ignore and try to stop consumer
    } finally {
      try {
        eventConsumer.stop();
      } catch (Exception e) {
        // log and proceed to exit
        LOG.warn("Event consumer failed to stop.", e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @WithSpan
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

  private Sequenced<? extends ChangeEvent> transformEvent(Sequenced<? extends ChangeEvent> event) throws Exception {
    switch (event.getEvent().getChangeType()) {
      case DDL:
        return transformDDLEvent((Sequenced<DDLEvent>) event);
      case DML:
        return transformDMLEvent((Sequenced<DMLEvent>) event);
      default:
        // this can only happen if there is a bug in the program
        throw new RuntimeException(String.format("Unknown change type {}", event.getEvent().getChangeType()));
    }
  }

  private Sequenced<DMLEvent> transformDMLEvent(Sequenced<DMLEvent> event) throws Exception {
    DMLEvent dmlEvent = event.getEvent();
    DMLOperation operation = dmlEvent.getOperation();
    String tableName = operation.getSchemaName() == null ? operation.getTableName() : operation.getSchemaName() + "."
                                                                                        + operation.getTableName();
    List<Transformation> columnTransformations = this.transformations.get(tableName);
    if (columnTransformations == null || columnTransformations.isEmpty()) {
      return event;
    }

    StructuredRecord row = dmlEvent.getRow();
    Schema schema = TransformationUtil.transformSchema(row.getSchema(), columnTransformations).toSchema();
    row = transform(row, columnTransformations, schema);
    StructuredRecord previousRow = dmlEvent.getPreviousRow();
    previousRow = transform(previousRow, columnTransformations, schema);

    return new Sequenced<>(DMLEvent.builder(dmlEvent).setRow(row).setPreviousRow(previousRow).build(),
                           event.getSequenceNumber());
  }

  private StructuredRecord transform(StructuredRecord row, List<Transformation> columnTransformations, Schema schema)
    throws Exception {
    if (row == null) {
      return row;
    }

    //can not use Collectors.toMap because some value may be null due to the known bug:
    //https://stackoverflow.com/questions/24630963/nullpointerexception-in-collectors-tomap-with-null-entry-values
    Map<String, Object> valuesMap =
      row.getSchema().getFields().stream().map(Schema.Field::getName)
        .collect(HashMap::new, (m, n) -> m.put(n, row.get(n)), HashMap::putAll);
    valuesMap = TransformationUtil.transformValue(valuesMap, columnTransformations);
    StructuredRecord.Builder transformedRow = StructuredRecord.builder(schema);

    valuesMap.entrySet().forEach(e -> transformedRow.set(e.getKey(), e.getValue()));
    return transformedRow.build();
  }


  private Sequenced<DDLEvent> transformDDLEvent(Sequenced<DDLEvent> event) throws Exception {
    DDLEvent ddlEvent = event.getEvent();
    DDLOperation operation = ddlEvent.getOperation();
    String tableName = operation.getSchemaName() == null ? operation.getTableName() : operation.getSchemaName() + "."
                                                                                        + operation.getTableName();
    if (tableName == null) {
      //a database or schema level operation
      return event;
    }
    List<Transformation> columnTransformations = this.transformations.get(tableName);
    if (columnTransformations == null || columnTransformations.isEmpty()) {
      return event;
    }
    Schema schema = ddlEvent.getSchema();
    if (schema == null) {
      return event;
    }
    return new Sequenced<>(TransformationUtil.transformDDLEvent(ddlEvent, columnTransformations));
  }

  private void startFromLastCommit() throws Exception {
    // create a new queue to protect against badly behaving event readers.
    // For example, suppose an event reader doesn't implement the 'stop' method and keeps emitting events forever.
    // If the queue is re-used for the next event reader, the queue will have events from the older, badly behaving
    // reader as well as those from the new reader.
    // To protect against this, a new queue is created and the worker switches over completely to only read from
    // this new queue. The old misbehaving reader will just block forever when its queue is full.
    eventQueue = new CapacityBoundedEventQueue(eventQueueSize, eventQueueCapacity);
    deltaContext.clearMetrics();

    OffsetAndSequence offsetAndSequence = deltaContext.loadOffset();
    offset = offsetAndSequence.getOffset();
    QueueingEventEmitter emitter = new QueueingEventEmitter(readerDefinition, offsetAndSequence.getSequenceNumber(),
                                                            eventQueue);

    LOG.info("Starting from last committed offset {}", offset.get());

    eventReader = source.createReader(readerDefinition, deltaContext, emitter);
    eventConsumer = target.createConsumer(deltaContext);

    LOG.info("Starting Event Reader...");
    eventReader.start(offset);
    LOG.info("Started Event Reader.");
    LOG.info("Starting Event Consumer...");
    eventConsumer.start();
    LOG.info("Started Event Consumer.");
  }

  @VisibleForTesting
  static Map<Integer, Set<TableId>> assignTables(DeltaConfig config) {
    ParallelismConfig parallelism = config.getParallelism();
    List<InstanceConfig> instances = parallelism.getInstances();
    Map<Integer, Set<TableId>> assignments = new HashMap<>();
    if (!instances.isEmpty()) {
      int instanceNum = 0;
      for (InstanceConfig instanceConfig : instances) {
        assignments.put(instanceNum, instanceConfig.getTables());
        instanceNum++;
      }
      return assignments;
    }

    Integer numInstances = config.getParallelism().getNumInstances();
    numInstances = numInstances == null ? 1 : numInstances;
    numInstances = Math.max(numInstances, 1);

    int instanceNum = 0;
    for (SourceTable table : config.getTables()) {
      Set<TableId> instanceTables = assignments.computeIfAbsent(instanceNum, key -> new HashSet<>());
      instanceTables.add(new TableId(table.getDatabase(), table.getTable(), table.getSchema()));
      instanceNum = (instanceNum + 1) % numInstances;
    }

    return assignments;
  }
}
