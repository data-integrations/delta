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

package io.cdap.delta.app;

import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.TestBase;
import io.cdap.cdap.test.WorkerManager;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaTarget;
import io.cdap.delta.app.proto.DeltaConfig;
import io.cdap.delta.app.proto.Plugin;
import io.cdap.delta.app.proto.Stage;
import io.cdap.delta.bigquery.BigQueryTarget;
import io.cdap.delta.mysql.MySqlDeltaSource;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Tests for delta pipelines.
 */
public class DeltaPipelineTest extends TestBase {
  private static final ArtifactId APP_ARTIFACT = NamespaceId.DEFAULT.artifact("app", "1.0.0");

  @BeforeClass
  public static void setupTest() throws Exception {
    addAppArtifact(APP_ARTIFACT, DeltaApp.class, DeltaSource.class.getPackage().getName());


    ArtifactId pluginArtifact = NamespaceId.DEFAULT.artifact("plugins", "1.0.0");
    // add plugins artifact
    addPluginArtifact(pluginArtifact, APP_ARTIFACT,
                      MySqlDeltaSource.class, BigQueryTarget.class);
  }

  @Test
  public void test() throws Exception {
    Map<String, String> sourceProperties = new HashMap<>();
    sourceProperties.put("host", "localhost");
    sourceProperties.put("port", "3306");
    sourceProperties.put("user", "cdap");
    sourceProperties.put("password", "cdap");
    sourceProperties.put("consumerID", "81323");
    Stage source = new Stage("source", new Plugin("mysql", DeltaSource.PLUGIN_TYPE, sourceProperties, null));

    Map<String, String> targetProperties = new HashMap<>();
    String accountKey = new String(Files.readAllBytes(Paths.get("/usr/local/google/home/ashau/ashau-dev0.json")));
    targetProperties.put("project", "ashau-dev0");
    targetProperties.put("serviceAccountKey", accountKey);
    targetProperties.put("stagingBucket", "ashau-delta-test");
    Stage target = new Stage("target", new Plugin("bigquery", DeltaTarget.PLUGIN_TYPE, targetProperties, null));

    DeltaConfig config = new DeltaConfig(source, target);
    AppRequest<DeltaConfig> appRequest =
      new AppRequest<>(new ArtifactSummary(APP_ARTIFACT.getArtifact(), APP_ARTIFACT.getVersion()), config);

    ApplicationId appId = NamespaceId.DEFAULT.app(UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkerManager workerManager = appManager.getWorkerManager(DeltaWorker.class.getSimpleName());
    workerManager.start();

    TimeUnit.SECONDS.sleep(300L);
  }
}
