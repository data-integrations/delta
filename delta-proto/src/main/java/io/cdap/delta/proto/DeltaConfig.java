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

package io.cdap.delta.proto;

import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.Resources;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Config for a Delta app.
 */
public class DeltaConfig extends Config {
  private final List<Stage> stages;
  private final List<Connection> connections;
  private final Resources resources;
  private final String offsetBasePath;
  private final boolean service;

  public DeltaConfig(Stage source, Stage target) {
    this(source, target, null, null);
  }

  public DeltaConfig(Stage source, Stage target, Resources resources, String offsetBasePath) {
    this.stages = Collections.unmodifiableList(Arrays.asList(source, target));
    this.connections = Collections.singletonList(new Connection(source.getName(), target.getName()));
    this.resources = resources;
    this.offsetBasePath = offsetBasePath;
    this.service = false;
  }

  public List<Stage> getStages() {
    return stages == null ? Collections.emptyList() : stages;
  }

  public String getOffsetBasePath() {
    return offsetBasePath == null || offsetBasePath.isEmpty() ? "cdap/delta/" : offsetBasePath;
  }

  public List<Connection> getConnections() {
    return connections == null ? Collections.emptyList() : connections;
  }

  public Resources getResources() {
    return resources == null ? new Resources(8192, 4) : resources;
  }

  public boolean isService() {
    return service;
  }
}
