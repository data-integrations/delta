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

import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.DeltaTargetContext;
import io.cdap.delta.api.Offset;

import java.io.IOException;
import java.util.Collections;
import javax.annotation.Nullable;

/**
 * Context for delta plugins
 */
public class DeltaContext implements DeltaSourceContext, DeltaTargetContext {
  private final DeltaPipelineId id;
  private final String runId;
  private final Metrics metrics;
  private final StateStore stateStore;
  private final PluginContext pluginContext;

  public DeltaContext(DeltaPipelineId id, String runId, Metrics metrics, StateStore stateStore,
                      PluginContext pluginContext) {
    this.id = id;
    this.runId = runId;
    this.metrics = metrics;
    this.stateStore = stateStore;
    this.pluginContext = pluginContext;
  }

  @Override
  public void commitOffset(Offset offset) throws IOException {
    stateStore.writeOffset(id, offset);
  }

  public Offset loadOffset() throws IOException {
    Offset offset = stateStore.readOffset(id);
    return offset == null ? new Offset(Collections.emptyMap()) : offset;
  }

  @Override
  public String getApplicationName() {
    return id.getApp();
  }

  @Override
  public String getRunId() {
    return runId;
  }

  @Override
  public Metrics getMetrics() {
    return metrics;
  }

  @Nullable
  @Override
  public byte[] getState(String key) throws IOException {
    return stateStore.readState(id, key);
  }

  @Override
  public void putState(String key, byte[] val) throws IOException {
    stateStore.writeState(id, key, val);
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId) {
    return pluginContext.getPluginProperties(pluginId);
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId, MacroEvaluator evaluator) throws InvalidMacroException {
    return pluginContext.getPluginProperties(pluginId, evaluator);
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    return pluginContext.loadPluginClass(pluginId);
  }

  @Override
  public <T> T newPluginInstance(String pluginId) throws InstantiationException {
    return pluginContext.newPluginInstance(pluginId);
  }

  @Override
  public <T> T newPluginInstance(String pluginId, MacroEvaluator evaluator)
    throws InstantiationException, InvalidMacroException {
    return pluginContext.newPluginInstance(pluginId, evaluator);
  }
}
