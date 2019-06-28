/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.delta.mysql;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.Offset;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.ddl.DdlParserListener;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Reads events from MySQL
 */
public class MySqlEventReader implements EventReader {
  public static final Logger LOG = LoggerFactory.getLogger(MySqlEventReader.class);
  private final MySqlConfig config;
  private final EventEmitter emitter;
  private final ExecutorService executorService;
  private final DeltaSourceContext context;
  private EmbeddedEngine engine;

  public MySqlEventReader(MySqlConfig config, DeltaSourceContext context, EventEmitter emitter) {
    this.config = config;
    this.context = context;
    this.emitter = emitter;
    this.executorService = Executors.newSingleThreadScheduledExecutor();
  }

  @Override
  public void start(Offset offset) {
    /*
        OffsetBackingStore is pretty weird... keys are ByteBuffer representations of Strings like:

        {"schema":null,"payload":["mysql-connector",{"server":"dummy"}]}

        and keys are ByteBuffer representations of Strings like:

        {"file":"mysql-bin.000003","pos":16838027}
     */

    String fileStr = Bytes.toString(offset.get().getOrDefault("file", Bytes.toBytes("")));
    byte[] posBytes = offset.get().get("pos");
    String pos = posBytes == null ? "" : Long.toString(Bytes.toLong(posBytes));
    // TODO: don't use EmbeddedEngine, these things aren't actually configurable
    // have to pass config to the offset storage implementation through 'offset.storage.file.filename'
    // since embedded engine only passes a hardcoded set of config properties to the offset store.
    Configuration debeziumConf = Configuration.create()
      .with("connector.class", MySqlConnector.class.getName())
      .with("offset.storage", ConstantOffsetBackingStore.class.getName())
      .with("offset.storage.file.filename", pos + "|" + fileStr)
      .with("offset.flush.interval.ms", 1000)
      /* begin connector properties */
      .with("name", "delta")
      .with("database.hostname", config.getHost())
      .with("database.port", config.getPort())
      .with("database.user", config.getUser())
      .with("database.password", config.getPassword())
      .with("server.id", config.getConsumerID())
      .with("database.history", DBSchemaHistory.class.getName())
      .with("database.whitelist", config.getDatabaseWhiteList())
      .with("database.server.name", "dummy") // this is the kafka topic for hosted debezium - it doesn't matter
      .with("database.serverTimezone", config.getServerTimezone())
      .build();
    MySqlConnectorConfig mysqlConf = new MySqlConnectorConfig(debeziumConf);
    DBSchemaHistory.deltaRuntimeContext = context;

    MySqlValueConverters mySqlValueConverters = getValueConverters(mysqlConf);
    DdlParser ddlParser = mysqlConf.getDdlParsingMode().getNewParserInstance(mySqlValueConverters, tableId -> true);

    /*
       For ddl, struct contains 3 top level fields:
         source struct
         databaseName string
         ddl string

       Before every DDL event, a weird event with ddl='# Dum' is consumed before the actual event.

       source is a struct with 14 fields:
         0 - version string (debezium version)
         1 - connector string
         2 - name string (name of the consumer, set when creating the Configuration)
         3 - server_id int64
         4 - ts_sec int64
         5 - gtid string
         6 - file string
         7 - pos int64     (there can be multiple events for the same file and position.
                            Everything in same position seems to be anything done in the same query)
         8 - row int32     (if multiple rows are involved in the same transaction, this is the row #)
         9 - snapshot boolean (null is the same as false)
         10 - thread int64
         11 - db string
         12 - table string
         13 - query string

       For dml, struct contains 5 top level fields:
         0 - before struct
         1 - after struct
         2 - source struct
         3 - op string (c for create or insert, u for update, d for delete, and r for read)
                        not sure when 'r' happens, it's not for select queries...
         4 - ts_ms int64 (this is *not* the timestamp of the event, but the timestamp when Debezium read it)
       before is a struct representing the row before the operation. It will have a schema matching the table schema
       after is a struct representing the row after the operation. It will have a schema matching the table schema
     */

    Tables tables = new Tables();
    ClassLoader oldCL = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    try {
      // Create the engine with this configuration ...
      engine = EmbeddedEngine.create()
        .using(debeziumConf)
        .notifying(sourceRecord -> {
          if (sourceRecord.value() == null) {
            return;
          }

          Map<String, ?> sourceOffset = sourceRecord.sourceOffset();
          String binlogFile = (String) sourceOffset.get("file");
          long binlogPosition = (Long) sourceOffset.get("pos");
          Map<String, byte[]> deltaOffset = new HashMap<>(2);
          deltaOffset.put("file", Bytes.toBytes(binlogFile));
          deltaOffset.put("pos", Bytes.toBytes(binlogPosition));
          Offset recordOffset = new Offset(deltaOffset);

          StructuredRecord val = Records.convert((Struct) sourceRecord.value());
          String ddl = val.get("ddl");
          if (ddl != null) {
            ddlParser.getDdlChanges().reset();
            ddlParser.parse(ddl, tables);
            ddlParser.getDdlChanges().groupEventsByDatabase((databaseName, events) -> {
              for (DdlParserListener.Event event : events) {
                DDLEvent.Builder builder = DDLEvent.builder()
                  .setDatabase(databaseName)
                  .setOffset(recordOffset);
                switch (event.type()) {
                  case ALTER_TABLE:
                    DdlParserListener.TableAlteredEvent alteredEvent = (DdlParserListener.TableAlteredEvent) event;
                    if (alteredEvent.previousTableId() != null) {
                      builder.setOperation(DDLOperation.RENAME_TABLE)
                        .setPrevTable(alteredEvent.previousTableId().table());
                    } else {
                      builder.setOperation(DDLOperation.ALTER_TABLE);
                    }
                    TableId tableId = alteredEvent.tableId();
                    Table table = tables.forTable(tableId);
                    emitter.emit(builder.setTable(tableId.table())
                                   .setSchema(Records.getSchema(table, mySqlValueConverters))
                                   .setPrimaryKey(table.primaryKeyColumnNames())
                                   .build());
                    break;
                  case DROP_TABLE:
                    DdlParserListener.TableDroppedEvent droppedEvent = (DdlParserListener.TableDroppedEvent) event;
                    emitter.emit(builder.setOperation(DDLOperation.DROP_TABLE)
                                   .setTable(droppedEvent.tableId().table())
                                   .build());
                    break;
                  case CREATE_TABLE:
                    DdlParserListener.TableCreatedEvent createdEvent = (DdlParserListener.TableCreatedEvent) event;
                    tableId = createdEvent.tableId();
                    table = tables.forTable(tableId);
                    emitter.emit(builder.setOperation(DDLOperation.CREATE_TABLE)
                                   .setTable(tableId.table())
                                   .setSchema(Records.getSchema(table, mySqlValueConverters))
                                   .setPrimaryKey(table.primaryKeyColumnNames())
                                   .build());
                    break;
                  case DROP_DATABASE:
                    emitter.emit(builder.setOperation(DDLOperation.DROP_DATABASE).build());
                    break;
                  case CREATE_DATABASE:
                    emitter.emit(builder.setOperation(DDLOperation.CREATE_DATABASE).build());
                    break;
                  case TRUNCATE_TABLE:
                    DdlParserListener.TableTruncatedEvent truncatedEvent =
                      (DdlParserListener.TableTruncatedEvent) event;
                    emitter.emit(builder.setOperation(DDLOperation.TRUNCATE_TABLE)
                                   .setTable(truncatedEvent.tableId().table())
                                   .build());
                    break;
                  default:
                    return;
                }

              }
            });
            return;
          }

          DMLOperation op;
          String opStr = val.get("op");
          if ("c".equals(opStr)) {
            op = DMLOperation.INSERT;
          } else if ("u".equals(opStr)) {
            op = DMLOperation.UPDATE;
          } else if ("d".equals(opStr)) {
            op = DMLOperation.DELETE;
          } else {
            LOG.warn("Skipping unknown operation type '{}'", opStr);
            return;
          }
          StructuredRecord source = val.get("source");
          String database = source.get("db");
          String table = source.get("table");
          String transactionId = source.get("gtid");
          if (transactionId == null) {
            // this is not really a transaction id, but we don't get an event when a transaction started/ended
            transactionId = String.format("%s:%d", source.get("file"), source.get("pos"));
          }

          StructuredRecord before = val.get("before");
          StructuredRecord after = val.get("after");
          Long ingestTime = val.get("ts_ms");
          if (op == DMLOperation.DELETE) {
            emitter.emit(new DMLEvent(recordOffset, op, database, table, before, transactionId, ingestTime));
          } else {
            emitter.emit(new DMLEvent(recordOffset, op, database, table, after, transactionId, ingestTime));
          }
        })
        .using((success, message, error) -> {
          if (!success) {
            LOG.error("Failed - {}", message, error);
          }
        })
        .build();
      executorService.submit(engine);
    } finally {
      Thread.currentThread().setContextClassLoader(oldCL);
    }
  }

  public void stop() throws InterruptedException {
    if (engine != null && engine.stop()) {
      engine.await(1, TimeUnit.MINUTES);
    }
    executorService.shutdown();
  }

  private static MySqlValueConverters getValueConverters(MySqlConnectorConfig configuration) {
    // Use MySQL-specific converters and schemas for values ...

    String timePrecisionModeStr = configuration.getConfig().getString(MySqlConnectorConfig.TIME_PRECISION_MODE);
    TemporalPrecisionMode timePrecisionMode = TemporalPrecisionMode.parse(timePrecisionModeStr);

    JdbcValueConverters.DecimalMode decimalMode = configuration.getDecimalMode();

    String bigIntUnsignedHandlingModeStr = configuration.getConfig()
      .getString(MySqlConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE);
    MySqlConnectorConfig.BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode =
      MySqlConnectorConfig.BigIntUnsignedHandlingMode.parse(bigIntUnsignedHandlingModeStr);
    JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode = bigIntUnsignedHandlingMode.asBigIntUnsignedMode();

    boolean timeAdjusterEnabled = configuration.getConfig().getBoolean(MySqlConnectorConfig.ENABLE_TIME_ADJUSTER);
    return new MySqlValueConverters(decimalMode, timePrecisionMode, bigIntUnsignedMode,
                                    timeAdjusterEnabled ? MySqlEventReader::adjustTemporal : x -> x);
  }

  private static Temporal adjustTemporal(Temporal temporal) {
    if (temporal.isSupported(ChronoField.YEAR)) {
      int year = temporal.get(ChronoField.YEAR);
      if (0 <= year && year <= 69) {
        temporal = temporal.plus(2000, ChronoUnit.YEARS);
      } else if (70 <= year && year <= 99) {
        temporal = temporal.plus(1900, ChronoUnit.YEARS);
      }
    }
    return temporal;
  }
}
