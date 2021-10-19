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

import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaTarget;
import io.cdap.transformation.api.Transformation;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A mock Configurer that returns a pre-defined source or target instance.
 */
public class MockConfigurer implements Configurer {
  private final DeltaSource source;
  private final DeltaTarget target;
  private final Map<String, Transformation> transformations;

  public MockConfigurer(DeltaSource source, DeltaTarget target) {
    this(source, target, null);
  }

  public MockConfigurer(DeltaSource source, DeltaTarget target, Map<String, Transformation> transformations) {
    this.source = source;
    this.target = target;
    this.transformations = transformations == null ? Collections.emptyMap() : transformations;
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                         PluginSelector selector) {
    if (DeltaSource.PLUGIN_TYPE.equals(pluginType)) {
      return (T) source;
    }
    if (DeltaTarget.PLUGIN_TYPE.equals(pluginType)) {
      return (T) target;
    }
    if (Transformation.PLUGIN_TYPE.equals(pluginType)) {
      return (T) transformations.get(pluginName);
    }

    return null;
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId,
                                     PluginProperties properties, PluginSelector selector) {
    if (DeltaSource.PLUGIN_TYPE.equals(pluginType)) {
      return (Class<T>) source.getClass();
    } else if (DeltaTarget.PLUGIN_TYPE.equals(pluginType)) {
      return (Class<T>) target.getClass();
    }
    return null;
  }
}
