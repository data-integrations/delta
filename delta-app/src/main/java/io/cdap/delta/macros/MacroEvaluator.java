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
package io.cdap.delta.macros;

import io.cdap.delta.proto.DeltaConfig;
import io.cdap.delta.proto.Plugin;
import io.cdap.delta.proto.Stage;
import io.cdap.delta.store.Namespace;

import java.util.Map;

public class MacroEvaluator {
 private final PropertyEvaluator propertyEvaluator;

 public MacroEvaluator(PropertyEvaluator propertyEvaluator) {
  this.propertyEvaluator = propertyEvaluator;
 }

 public DeltaConfig evaluateMacros(Namespace namespace, DeltaConfig config) {
  DeltaConfig.Builder builder = DeltaConfig.builder()
    .setDescription(config.getDescription())
    .setResources(config.getResources())
    .setSource(config.getSource())
    .setOffsetBasePath(config.getOffsetBasePath())
    .setTables(config.getTables())
    .setTableTransformations(config.getTableTransformations())
    .setSource(evaluateMacros(namespace.getName(), config.getSource()));

  Stage target = config.getTarget();
  if (target != null) {
   builder.setTarget(evaluateMacros(namespace.getName(), target));
  }

  return builder.build();
 }

 private Stage evaluateMacros(String namespace, Stage stage) {
  Plugin plugin = stage.getPlugin();
  Map<String, String> evaluatedProperties = propertyEvaluator.evaluate(namespace, plugin.getProperties());
  Plugin evaluatedPlugin = new Plugin(plugin.getName(), plugin.getType(), evaluatedProperties, plugin.getArtifact());
  return new Stage(stage.getName(), evaluatedPlugin);
 }
}
