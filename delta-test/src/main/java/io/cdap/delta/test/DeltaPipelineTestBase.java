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

package io.cdap.delta.test;

import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.test.TestBase;
import io.cdap.delta.api.ChangeEvent;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.test.mock.FailureTarget;
import io.cdap.delta.test.mock.MockErrorTarget;
import io.cdap.delta.test.mock.MockSource;
import io.cdap.delta.test.mock.MockTarget;

import java.util.HashSet;
import java.util.Set;

/**
 * Test base for delta pipelines.
 */
public class DeltaPipelineTestBase extends TestBase {
  protected static final ArtifactId ARTIFACT_ID = new ArtifactId("default", "delta-app", "1.0.0");
  protected static final ArtifactSummary ARTIFACT_SUMMARY = new ArtifactSummary(ARTIFACT_ID.getArtifact(),
                                                                                ARTIFACT_ID.getVersion());

  protected static void setupArtifacts(Class<?> appClass) throws Exception {
    // add the app artifact
    addAppArtifact(ARTIFACT_ID, appClass,
                   // these are the packages that should be exported so that they are visible to plugins
                   ChangeEvent.class.getPackage().getName(),
                   TableDetail.class.getPackage().getName());

    // add plugins artifact
    ArtifactId mocksArtifactId = new ArtifactId("default", "mocks", "1.0.0");
    // need to specify each PluginClass so that they can be used outside of this project. If we don't do this,
    // when the plugin jar is created, it will add lib/delta-test.jar to the plugin jar.
    // The ArtifactInspector will not examine any library jars for plugins, because it assumes plugins are always
    // .class files in the jar and never in the dependencies, which is normally a reasonable assumption.
    // So since the plugins are in lib/delta-test.jar, CDAP won't find any plugins in the jar.
    // To work around, we'll just explicitly specify each plugin
    Set<PluginClass> pluginClasses = new HashSet<>();
    pluginClasses.add(MockSource.PLUGIN_CLASS);
    pluginClasses.add(MockTarget.PLUGIN_CLASS);
    pluginClasses.add(MockErrorTarget.PLUGIN_CLASS);
    pluginClasses.add(FailureTarget.PLUGIN_CLASS);
    addPluginArtifact(mocksArtifactId, ARTIFACT_ID, pluginClasses,
                      MockSource.class, MockTarget.class, MockErrorTarget.class, FailureTarget.class);
    enableCapability("cdc");
  }

  protected static void removeArtifacts() throws Exception {
    removeCapability("cdc");
  }
}
