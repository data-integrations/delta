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

package io.cdap.delta.app.service;

import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.SourceConfigurer;
import io.cdap.delta.api.SourceProperties;

import javax.annotation.Nullable;

/**
 * Default implementation of {@link SourceConfigurer}.
 */
public class DefaultSourceConfigurer implements SourceConfigurer {
  private final Configurer delegate;
  private SourceProperties sourceProperties;

  public DefaultSourceConfigurer(Configurer delegate) {
    this.delegate = delegate;
  }

  @Override
  public void setProperties(SourceProperties sourceProperties) {
    this.sourceProperties = sourceProperties;
  }

  @Nullable
  public SourceProperties getSourceProperties() {
    return sourceProperties;
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
    return delegate.getNamespace();
  }

  @Nullable
  @Override
  public String getName() {
    return delegate.getName();
  }
}
