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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.DeltaPipelineId;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.app.DeltaWorkerId;
import io.cdap.delta.app.PipelineConfigService;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static io.cdap.delta.app.PipelineConfigService.AGGREGATE_STATS_FREQUENCY_ARG;

@RunWith(MockitoJUnitRunner.class)
public class MetricsHandlerTest {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(MetricsHandlerTest.class);
  private static final String DB = "db";
  public static final String TABLE_1 = "table1";
  public static final String TABLE_2 = "table2";
  private static final String SCHEMA = "schema";
  private static final long INGEST_TIMESTAMP = 1000L;
  private static final int SIZE_IN_BYTES = 100;
  private static Logger metricsLogger = (Logger) LoggerFactory.getLogger(MetricsHandler.class);
  private static ListAppender<ILoggingEvent> listAppender;

  @Mock
  private Metrics metrics;
  private MetricsHandler metricsHandler;

  @BeforeClass
  public static void setup() {
    listAppender = new ListAppender<>();
    listAppender.start();

    metricsLogger.addAppender(listAppender);
  }

  @Before
  public void setupTest() {
    DeltaPipelineId pipelineId = new DeltaPipelineId("test", "testapp", 1L);
    DeltaWorkerId deltaWorkerId = new DeltaWorkerId(pipelineId, 1);

    List<SourceTable> tables = new ArrayList<>();
    tables.add(new SourceTable(DB, TABLE_1, SCHEMA, Collections.emptySet(),
                               Collections.emptySet(), Collections.emptySet()));
    tables.add(new SourceTable(DB, TABLE_2));
    Map<String, String> runtimeArgs = new HashMap<>();
    runtimeArgs.put(AGGREGATE_STATS_FREQUENCY_ARG, "4");

    PipelineConfigService configService = new PipelineConfigService(runtimeArgs);

    Mockito.when(metrics.child(Mockito.any())).thenReturn(metrics);

    metricsHandler = new MetricsHandler(deltaWorkerId, metrics, tables, configService);

    metricsHandler.clearMetrics();
    listAppender.list.clear();
  }

  @After
  public void tearDown() throws InterruptedException {
    metricsHandler.close();
  }


  @Test
  public void testAggregatePublishStatsDDLAndDML() {
    metricsHandler.incrementPublishCount(
      new DMLOperation(DB, SCHEMA, TABLE_1, DMLOperation.Type.INSERT, INGEST_TIMESTAMP, SIZE_IN_BYTES));
    metricsHandler.incrementPublishCount(
      new DMLOperation(DB, SCHEMA, TABLE_1, DMLOperation.Type.UPDATE, INGEST_TIMESTAMP, 200));
    metricsHandler.incrementPublishCount(
      new DMLOperation(DB, SCHEMA, TABLE_1, DMLOperation.Type.UPDATE, INGEST_TIMESTAMP, 200));
    metricsHandler.incrementPublishCount(
      new DDLOperation(DB, SCHEMA, TABLE_1, DDLOperation.Type.ALTER_TABLE));
    metricsHandler.incrementPublishCount(
      new DMLOperation(DB, null, TABLE_2, DMLOperation.Type.INSERT, INGEST_TIMESTAMP, SIZE_IN_BYTES));
    metricsHandler.incrementPublishCount(
      new DMLOperation(DB, null, TABLE_2, DMLOperation.Type.INSERT, INGEST_TIMESTAMP, SIZE_IN_BYTES));


    Awaitility.await()
      .atMost(Duration.ofSeconds(10))
      .pollDelay(Duration.ofSeconds(1))
      .until(() -> {
        List<ILoggingEvent> logs = listAppender.list;

        Predicate<ILoggingEvent> matchSummary = log -> {
          String message = log.getFormattedMessage();
          return
            message.contains("SourcePlugin") &&
              message.contains("Summary") &&
              message.contains("DML Events=5") &&
              message.contains("DDL Events=1");
        };

        Predicate<ILoggingEvent> matchTable1 = log -> {
          String message = log.getFormattedMessage();
          return
            message.contains("SourcePlugin") &&
              message.contains(TABLE_1) &&
              message.contains("DDL Events=1") &&
              message.contains("INSERT:1") &&
              message.contains("UPDATE:2");
        };

        Predicate<ILoggingEvent> matchTable2 = log -> {
          String message = log.getFormattedMessage();
          return
            message.contains("SourcePlugin") &&
              message.contains(TABLE_2) &&
              message.contains("DDL Events=0") &&
              message.contains("INSERT:2") &&
              message.contains("UPDATE:0") &&
              message.contains("DELETE:0");
        };

        return matchLogs(logs, matchSummary, matchTable1, matchTable2);
      });
  }

  @Test
  public void testAggregateConsumeStatsDDLAndDML() {
    metricsHandler.incrementConsumeCount(
      new DMLOperation(DB, SCHEMA, TABLE_1, DMLOperation.Type.INSERT, INGEST_TIMESTAMP, SIZE_IN_BYTES));
    metricsHandler.incrementConsumeCount(
      new DMLOperation(DB, SCHEMA, TABLE_1, DMLOperation.Type.UPDATE, INGEST_TIMESTAMP, SIZE_IN_BYTES));
    metricsHandler.incrementConsumeCount(
      new DMLOperation(DB, SCHEMA, TABLE_1, DMLOperation.Type.UPDATE, INGEST_TIMESTAMP, SIZE_IN_BYTES));
    metricsHandler.incrementConsumeCount(
      new DDLOperation(DB, SCHEMA, TABLE_1, DDLOperation.Type.ALTER_TABLE));
    metricsHandler.incrementConsumeCount(
      new DMLOperation(DB, null, TABLE_2, DMLOperation.Type.INSERT, INGEST_TIMESTAMP, SIZE_IN_BYTES));
    metricsHandler.incrementConsumeCount(
      new DMLOperation(DB, null, TABLE_2, DMLOperation.Type.INSERT, INGEST_TIMESTAMP, SIZE_IN_BYTES));


    metricsHandler.emitMetrics();

    //Below events should not be aggregated as aggregation should happen on emitMetrics
    metricsHandler.incrementConsumeCount(
      new DMLOperation(DB, SCHEMA, TABLE_1, DMLOperation.Type.UPDATE, INGEST_TIMESTAMP, SIZE_IN_BYTES));
    metricsHandler.incrementConsumeCount(
      new DMLOperation(DB, SCHEMA, TABLE_1, DMLOperation.Type.UPDATE, INGEST_TIMESTAMP, SIZE_IN_BYTES));


    Awaitility.await()
      .atMost(Duration.ofSeconds(10))
      .pollDelay(Duration.ofSeconds(1))
      .until(() -> {
        List<ILoggingEvent> logs = listAppender.list;

        Predicate<ILoggingEvent> matchSummary = log -> {
          String message = log.getFormattedMessage();
          return
            message.contains("TargetPlugin") &&
              message.contains("Summary") &&
              message.contains("DML Events=5") &&
              message.contains("DDL Events=1");
        };

        Predicate<ILoggingEvent> matchTable1 = log -> {
          String message = log.getFormattedMessage();
          return
            message.contains("TargetPlugin") &&
              message.contains(TABLE_1) &&
              message.contains("DDL Events=1") &&
              message.contains("INSERT:1") &&
              message.contains("UPDATE:2");
        };

        Predicate<ILoggingEvent> matchTable2 = log -> {
          String message = log.getFormattedMessage();
          return
            message.contains("TargetPlugin") &&
              message.contains(TABLE_2) &&
              message.contains("DDL Events=0") &&
              message.contains("INSERT:2") &&
              message.contains("UPDATE:0") &&
              message.contains("DELETE:0");
        };

        return matchLogs(logs, matchSummary, matchTable1, matchTable2);
      });
  }

  private Boolean matchLogs(List<ILoggingEvent> logs, Predicate<ILoggingEvent>... conditions) {
    for (int i = 0; i < conditions.length; i++) {
      Predicate condition = conditions[i];
      boolean matches = logs.stream().anyMatch(condition);
      if (!matches) {
        return false;
      }
    }
    return true;
  }
}
