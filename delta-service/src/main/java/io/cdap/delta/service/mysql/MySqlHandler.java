/*
 *  Copyright © 2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.delta.service.mysql;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.service.http.AbstractSystemHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;


/**
 * MySQL Handler
 */
public class MySqlHandler extends AbstractSystemHttpServiceHandler {
  private static final Gson GSON = new GsonBuilder().create();
  private static final Charset charset = StandardCharsets.UTF_8;


  @Override
  public void initialize(SystemHttpServiceContext context) throws Exception {
    super.initialize(context);
    Class.forName("com.mysql.jdbc.Driver"); // load mysql jdbc driver
  }

  private Map<String, String> getRequestContent(HttpServiceRequest request) {
    ByteBuffer content = request.getContent();
    String decodedContent = charset.decode(content).toString();

    Map<String, String> object = GSON.fromJson(decodedContent, HashMap.class);
    return object;
  }

  private Connection getConnection(HttpServiceRequest request, String db) throws SQLException {
    Map<String, String> content = getRequestContent(request);
    String hostname = content.get("host");
    String port = content.get("port");
    String user = content.get("user");
    String password = content.get("password");

    String connectionString = String.format("jdbc:mysql://%s:%s/%s?serverTimezone=UTC", hostname, port, db);

    return DriverManager.getConnection(connectionString, user, password);
  }

  @POST
  @Path("contexts/{context}/mysql/database/{db}/tables")
  public void getTables(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("db") String db)
          throws SQLException {
    Connection connection = getConnection(request, db);
    DatabaseMetaData meta = connection.getMetaData();

    ResultSet rs = meta.getTables(db, null, "%", null);

    List<String> values = new ArrayList<>();

    while (rs.next()) {
      String name = rs.getString("TABLE_NAME");
      values.add(name);
    }

    rs.close();

    responder.sendJson(values);
  }

  @POST
  @Path("contexts/{context}/mysql/database/{db}/tables/{table}")
  public void getTableInfo(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("table") String table, @PathParam("db") String db)
          throws SQLException {
    Connection connection = getConnection(request, db);

    Statement statement = connection.createStatement();
    ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s LIMIT 100", table));

    List<Object> columns = getColumnsInfo(connection.getMetaData(), db, table);

    List<Object> data = new ArrayList<>();
    ResultSetMetaData meta = rs.getMetaData();
    while (rs.next()) {
      Map<String, Object> row = new HashMap<>();
      for (int i = 1; i < meta.getColumnCount() + 1; i++) {
        Object object = rs.getObject(i);
        row.put(meta.getColumnName(i), object);
      }
      data.add(row);
    }

    Map<String, Object> response = new HashMap<>();
    response.put("columns", columns);
    response.put("data", data);

    rs.close();

    responder.sendJson(response);
  }

  @POST
  @Path("contexts/{context}/mysql/database/{db}")
  public void getDBInfo(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("db") String db)
          throws SQLException {
    Connection connection = getConnection(request, db);

    DatabaseMetaData dbMeta = connection.getMetaData();
    ResultSet table = dbMeta.getTables(db, null, "%", null);

    List<Object> response = new ArrayList<>();
    while (table.next()) {
      Map<String, Object> tableInfo = new HashMap<>();
      String tableName = table.getString("TABLE_NAME");
      List<Object> columns = getColumnsInfo(dbMeta, db, tableName);

      tableInfo.put("table", tableName);
      tableInfo.put("columns", columns);

      response.add(tableInfo);
    }

    table.close();

    responder.sendJson(response);
  }

  private List<Object> getColumnsInfo(DatabaseMetaData dbMeta, String db, String tableName) throws SQLException {
    List<Object> columns = new ArrayList<>();

    ResultSet columnRs = dbMeta.getColumns(db, null, tableName, null);
    while (columnRs.next()) {
      Map<String, String> column = new HashMap<>();
      column.put("name", columnRs.getString("COLUMN_NAME"));
      column.put("type", columnRs.getString("TYPE_NAME"));
      columns.add(column);
    }
    columnRs.close();

    return columns;
  }
}
