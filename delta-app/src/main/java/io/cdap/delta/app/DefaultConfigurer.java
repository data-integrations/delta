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

import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.delta.api.Configurer;

import javax.annotation.Nullable;

/**
 *
 */
public class DefaultConfigurer implements Configurer {
  private final PluginConfigurer delegate;
  private final Long generation;
  private final String namespace;
  private final String name;

  public DefaultConfigurer(PluginConfigurer delegate) {
    this(delegate, null, null, null);
  }

  public DefaultConfigurer(PluginConfigurer delegate, @Nullable String namespace, @Nullable String name,
    @Nullable Long generation) {
    this.delegate = delegate;
    this.namespace = namespace;
    this.name = name;
    this.generation = generation;
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties) {
    return delegate.usePlugin(pluginType, pluginName, pluginId, properties);
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                         PluginSelector selector) {
    return delegate.usePlugin(pluginType, pluginName, pluginId, properties, selector);
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId,
                                     PluginProperties properties) {
    return delegate.usePluginClass(pluginType, pluginName, pluginId, properties);
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                                     PluginSelector selector) {
    return delegate.usePluginClass(pluginType, pluginName, pluginId, properties, selector);
  }

  @Nullable
  @Override
  public String getNamespace() {
    return namespace;
  }

  @Nullable
  @Override
  public String getName() {
    return name;
  }

  @Nullable
  @Override
  public Long getGeneration() {
    return generation;
  }
}
