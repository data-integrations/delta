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

package io.cdap.delta.test.mock;

import com.google.gson.Gson;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.ApplicationConfigUpdateAction;
import io.cdap.cdap.api.app.ApplicationUpdateContext;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.artifact.ArtifactVersionRange;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.proto.Artifact;
import io.cdap.delta.proto.DeltaConfig;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Mock implementation of update context for testing upgrade.
 */
public class MockApplicationUpdateContext implements ApplicationUpdateContext {
  private final Gson gson = new Gson();
  private final DeltaConfig originalConfig;
  private final Artifact newSource;
  private final Artifact newTarget;

  public MockApplicationUpdateContext(DeltaConfig originalConfig, Artifact newSource, Artifact newTarget) {
    this.originalConfig = originalConfig;
    this.newSource = newSource;
    this.newTarget = newTarget;
  }

  @Override
  public List<ApplicationConfigUpdateAction> getUpdateActions() {
    return Collections.singletonList(ApplicationConfigUpdateAction.UPGRADE_ARTIFACT);
  }

  @Override
  public <C extends Config> C getConfig(Type type) {
    return (C) originalConfig;
  }

  @Override
  public String getConfigAsString() {
    return gson.toJson(originalConfig);
  }

  @Override
  public List<ArtifactId> getPluginArtifacts(String pluginType, String pluginName,
                                             @Nullable ArtifactVersionRange pluginRange,
                                             int limit) throws Exception {
    if (pluginType.equals(DeltaSource.PLUGIN_TYPE)) {
      return Collections.singletonList(new ArtifactId(newSource.getName(), new ArtifactVersion(newSource.getVersion()),
                                                      ArtifactScope.valueOf(newSource.getScope())));
    }
    return Collections.singletonList(new ArtifactId(newTarget.getName(), new ArtifactVersion(newTarget.getVersion()),
                                                    ArtifactScope.valueOf(newTarget.getScope())));
  }
}
