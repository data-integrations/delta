/*
 * Copyright © 2022 Cask Data, Inc.
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
package io.cdap.delta.app.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.api.service.worker.RemoteExecutionException;
import io.cdap.cdap.api.service.worker.RemoteTaskException;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaTarget;
import io.cdap.delta.app.service.common.AbstractAssessorHandler;
import io.cdap.delta.app.service.remote.RemoteAssessPipelineTask;
import io.cdap.delta.app.service.remote.RemoteAssessTableTask;
import io.cdap.delta.app.service.remote.RemoteListTablesTask;
import io.cdap.delta.macros.ConfigMacroEvaluator;
import io.cdap.delta.proto.Artifact;
import io.cdap.delta.proto.DBTable;
import io.cdap.delta.proto.DeltaConfig;
import io.cdap.delta.proto.Plugin;
import io.cdap.delta.proto.Stage;
import io.cdap.delta.proto.TableAssessmentRequest;
import io.cdap.delta.store.Draft;
import io.cdap.delta.store.DraftService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.util.Collections;

@RunWith(PowerMockRunner.class)
@PrepareForTest({AbstractAssessorHandler.class})
public class AssessmentHandlerTest {
 private static final String NAMESPACE = "namespace";
 private static final String DRAFT = "draft";
 private static final String SOURCE_PLUGIN = "source";
 private static final String SCOPE = "system";
 private static final String TARGET_PLUGIN = "target";
 private static final int GENERATION = 10000;
 private static final String DESCRIPTION = "desc";
 private static final String VERSION = "1.0";
 private static final String REMOTE_TASK_RESPONSE_SUCCESS = "success";
 private static final Gson GSON = new GsonBuilder().create();
 private static final String DATABASE = "db";
 private static final String SCHEMA = "schema";
 private static final String TABLE = "table";

 @Mock
 private HttpServiceRequest request;
 @Mock
 private HttpServiceResponder response;
 @Mock
 private SystemHttpServiceContext remoteEnabledContext;
 @Mock
 private SystemHttpServiceContext context;
 @Mock
 private Admin admin;
 @Mock
 private DraftService draftService;
 @Mock
 private Draft draft;

 private DeltaConfig draftConfig;

 private AssessmentHandler remoteAssessmentHandler = new AssessmentHandler();
 private AssessmentHandler assessmentHandler = new AssessmentHandler();

 @Before
 public void init() throws Exception {
  Mockito.when(context.getAdmin()).thenReturn(admin);
  Mockito.when(remoteEnabledContext.getAdmin()).thenReturn(admin);

  Mockito.when(admin.getNamespaceSummary(NAMESPACE))
    .thenReturn(new NamespaceSummary(NAMESPACE, DESCRIPTION, GENERATION));
  assessmentHandler.initialize(context);

  Mockito.when(remoteEnabledContext.isRemoteTaskEnabled()).thenReturn(true);
  remoteAssessmentHandler.initialize(remoteEnabledContext);

  PowerMockito.whenNew(DraftService.class)
    .withArguments(Mockito.any(TransactionRunner.class), Mockito.any(ConfigMacroEvaluator.class))
    .thenReturn(draftService);
  Mockito.when(draftService.getDraft(Mockito.any())).thenReturn(draft);

  Plugin sourcePlugin = new Plugin(SOURCE_PLUGIN, DeltaSource.PLUGIN_TYPE,
                                   Collections.emptyMap(), new Artifact(SOURCE_PLUGIN, VERSION, SCOPE));
  Plugin targetPlugin = new Plugin(TARGET_PLUGIN, DeltaTarget.PLUGIN_TYPE,
                                   Collections.emptyMap(), new Artifact(SOURCE_PLUGIN, VERSION, SCOPE));
  draftConfig = DeltaConfig.builder()
    .setSource(new Stage(SOURCE_PLUGIN, sourcePlugin))
    .setTarget(new Stage(TARGET_PLUGIN, targetPlugin))
    .build();
  Mockito.when(draft.getConfig()).thenReturn(draftConfig);
 }

 @Test
 public void testListDraftTablesRemote() throws Exception {
  String remoteTaskResponseSuccess = REMOTE_TASK_RESPONSE_SUCCESS;
  Mockito.when(remoteEnabledContext.runTask(Mockito.any())).thenReturn(remoteTaskResponseSuccess.getBytes());
  ArgumentCaptor<RunnableTaskRequest> captor = ArgumentCaptor.forClass(RunnableTaskRequest.class);

  remoteAssessmentHandler.listDraftTables(request, response, NAMESPACE, DRAFT);

  Mockito.verify(remoteEnabledContext, Mockito.times(1)).runTask(captor.capture());
  Mockito.verify(response, Mockito.times(1)).sendString(remoteTaskResponseSuccess);

  RunnableTaskRequest taskRequest = captor.getValue();
  Assert.assertEquals(RemoteListTablesTask.class.getName(), taskRequest.getClassName());
 }

 @Test
 public void testListDraftTablesRemoteException() throws Exception {
  String remoteTaskResponseSuccess = REMOTE_TASK_RESPONSE_SUCCESS;
  Mockito.when(remoteEnabledContext.runTask(Mockito.any())).thenThrow(new NullPointerException("error"));
  ArgumentCaptor<RunnableTaskRequest> captor = ArgumentCaptor.forClass(RunnableTaskRequest.class);

  remoteAssessmentHandler.listDraftTables(request, response, NAMESPACE, DRAFT);

  Mockito.verify(remoteEnabledContext, Mockito.times(1)).runTask(captor.capture());
  Mockito.verify(response, Mockito.times(1)).sendError(HttpURLConnection.HTTP_INTERNAL_ERROR,
                                                       "error");

  RunnableTaskRequest taskRequest = captor.getValue();
  Assert.assertEquals(RemoteListTablesTask.class.getName(), taskRequest.getClassName());
 }

 @Test
 public void testListDraftTables() throws Exception {
  assessmentHandler.listDraftTables(request, response, NAMESPACE, DRAFT);

  Mockito.verify(remoteEnabledContext, Mockito.times(0)).runTask(Mockito.any());
  Mockito.verify(response, Mockito.times(1)).sendString(Mockito.anyString());
 }

 @Test
 public void testListDraftTablesRemoteIllegalArugumentException() throws Exception {
  String remoteTaskResponseSuccess = REMOTE_TASK_RESPONSE_SUCCESS;
  RemoteTaskException cause = new RemoteTaskException(IllegalArgumentException.class.getName(), "error",
                                                      new IllegalArgumentException("error"));
  RemoteExecutionException remoteExecutionException = new RemoteExecutionException(cause);
  Mockito.when(remoteEnabledContext.runTask(Mockito.any())).thenThrow(remoteExecutionException);
  ArgumentCaptor<RunnableTaskRequest> captor = ArgumentCaptor.forClass(RunnableTaskRequest.class);

  remoteAssessmentHandler.listDraftTables(request, response, NAMESPACE, DRAFT);

  Mockito.verify(remoteEnabledContext, Mockito.times(1)).runTask(captor.capture());
  Mockito.verify(response, Mockito.times(1)).sendError(HttpURLConnection.HTTP_BAD_REQUEST,
                                                       "error");

  RunnableTaskRequest taskRequest = captor.getValue();
  Assert.assertEquals(RemoteListTablesTask.class.getName(), taskRequest.getClassName());
 }

 @Test
 public void testListDraftTablesRemoteGenericException() throws Exception {
  String remoteTaskResponseSuccess = REMOTE_TASK_RESPONSE_SUCCESS;
  RemoteTaskException cause = new RemoteTaskException(IllegalStateException.class.getName(), "error",
                                                      new IllegalStateException("error"));
  RemoteExecutionException remoteExecutionException = new RemoteExecutionException(cause);
  Mockito.when(remoteEnabledContext.runTask(Mockito.any())).thenThrow(remoteExecutionException);
  ArgumentCaptor<RunnableTaskRequest> captor = ArgumentCaptor.forClass(RunnableTaskRequest.class);

  remoteAssessmentHandler.listDraftTables(request, response, NAMESPACE, DRAFT);

  Mockito.verify(remoteEnabledContext, Mockito.times(1)).runTask(captor.capture());
  Mockito.verify(response, Mockito.times(1)).sendError(HttpURLConnection.HTTP_INTERNAL_ERROR,
                                                       "error");

  RunnableTaskRequest taskRequest = captor.getValue();
  Assert.assertEquals(RemoteListTablesTask.class.getName(), taskRequest.getClassName());
 }

 @Test
 public void testAssessDraftRemote() throws Exception {
  Mockito.when(remoteEnabledContext.runTask(Mockito.any())).thenReturn(REMOTE_TASK_RESPONSE_SUCCESS.getBytes());
  ArgumentCaptor<RunnableTaskRequest> captor = ArgumentCaptor.forClass(RunnableTaskRequest.class);

  remoteAssessmentHandler.assessDraft(request, response, NAMESPACE, DRAFT);

  Mockito.verify(remoteEnabledContext, Mockito.times(1)).runTask(captor.capture());
  Mockito.verify(response, Mockito.times(1)).sendString(REMOTE_TASK_RESPONSE_SUCCESS);

  RunnableTaskRequest taskRequest = captor.getValue();
  Assert.assertEquals(RemoteAssessPipelineTask.class.getName(), taskRequest.getClassName());
 }


 @Test
 public void testAssessDraft() throws Exception {
  assessmentHandler.assessDraft(request, response, NAMESPACE, DRAFT);

  Mockito.verify(remoteEnabledContext, Mockito.times(0)).runTask(Mockito.any());
  Mockito.verify(response, Mockito.times(1)).sendString(Mockito.anyString());
 }

 @Test
 public void testAssessDeltaConfigRemote() throws Exception {
  Mockito.when(remoteEnabledContext.runTask(Mockito.any())).thenReturn(REMOTE_TASK_RESPONSE_SUCCESS.getBytes());

  setRequestContent(request, draftConfig);

  remoteAssessmentHandler.assessDeltaConfig(request, response, NAMESPACE);

  ArgumentCaptor<RunnableTaskRequest> captor = ArgumentCaptor.forClass(RunnableTaskRequest.class);
  Mockito.verify(remoteEnabledContext, Mockito.times(1)).runTask(captor.capture());
  Mockito.verify(response, Mockito.times(1)).sendString(REMOTE_TASK_RESPONSE_SUCCESS);

  RunnableTaskRequest taskRequest = captor.getValue();
  Assert.assertEquals(RemoteAssessPipelineTask.class.getName(), taskRequest.getClassName());
 }

 @Test
 public void testAssessDeltaConfig() throws Exception {
  setRequestContent(request, draftConfig);

  assessmentHandler.assessDeltaConfig(request, response, NAMESPACE);

  ArgumentCaptor<RunnableTaskRequest> captor = ArgumentCaptor.forClass(RunnableTaskRequest.class);

  Mockito.verify(remoteEnabledContext, Mockito.times(0)).runTask(Mockito.any());
  Mockito.verify(response, Mockito.times(1)).sendString(Mockito.anyString());
 }

 @Test
 public void testAssessDraftTableRemote() throws Exception {
  Mockito.when(remoteEnabledContext.runTask(Mockito.any())).thenReturn(REMOTE_TASK_RESPONSE_SUCCESS.getBytes());

  DBTable dbTable = new DBTable(DATABASE, SCHEMA, TABLE);
  setRequestContent(request, dbTable);

  remoteAssessmentHandler.assessTable(request, response, NAMESPACE, DRAFT);

  ArgumentCaptor<RunnableTaskRequest> captor = ArgumentCaptor.forClass(RunnableTaskRequest.class);
  Mockito.verify(remoteEnabledContext, Mockito.times(1)).runTask(captor.capture());
  Mockito.verify(response, Mockito.times(1)).sendString(REMOTE_TASK_RESPONSE_SUCCESS);

  RunnableTaskRequest taskRequest = captor.getValue();
  Assert.assertEquals(RemoteAssessTableTask.class.getName(), taskRequest.getClassName());
 }

 @Test
 public void testAssessDraftTable() throws Exception {
  DBTable dbTable = new DBTable(DATABASE, SCHEMA, TABLE);
  setRequestContent(request, dbTable);

  assessmentHandler.assessTable(request, response, NAMESPACE, DRAFT);
  Mockito.verify(remoteEnabledContext, Mockito.times(0)).runTask(Mockito.any());
  Mockito.verify(response, Mockito.times(1)).sendString(Mockito.anyString());
 }

 @Test
 public void testAssessTableRemote() throws Exception {
  Mockito.when(remoteEnabledContext.runTask(Mockito.any())).thenReturn(REMOTE_TASK_RESPONSE_SUCCESS.getBytes());

  DBTable dbTable = new DBTable(DATABASE, SCHEMA, TABLE);
  setRequestContent(request, new TableAssessmentRequest(dbTable, draftConfig));

  remoteAssessmentHandler.assessTableWithDeltaConfig(request, response, NAMESPACE);

  ArgumentCaptor<RunnableTaskRequest> captor = ArgumentCaptor.forClass(RunnableTaskRequest.class);
  Mockito.verify(remoteEnabledContext, Mockito.times(1)).runTask(captor.capture());
  Mockito.verify(response, Mockito.times(1)).sendString(REMOTE_TASK_RESPONSE_SUCCESS);

  RunnableTaskRequest taskRequest = captor.getValue();
  Assert.assertEquals(RemoteAssessTableTask.class.getName(), taskRequest.getClassName());
 }

 @Test
 public void testAssessTable() throws Exception {
  DBTable dbTable = new DBTable(DATABASE, SCHEMA, TABLE);
  setRequestContent(request, new TableAssessmentRequest(dbTable, draftConfig));

  assessmentHandler.assessTableWithDeltaConfig(request, response, NAMESPACE);

  Mockito.verify(remoteEnabledContext, Mockito.times(0)).runTask(Mockito.any());
  Mockito.verify(response, Mockito.times(1)).sendString(Mockito.anyString());
 }

 private void setRequestContent(HttpServiceRequest request, Object obj) {
  Mockito.when(request.getContent()).thenReturn(ByteBuffer.wrap(GSON.toJson(obj).getBytes()));
 }
}
