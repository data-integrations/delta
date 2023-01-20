/*
 * Copyright © 2020-2022 Cask Data, Inc.
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

package io.cdap.delta.store;

import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.delta.macros.PropertyEvaluator;

import java.util.Map;

/**
 * Evaluates macros in a map of properties.
 */
public class SystemServicePropertyEvaluator implements PropertyEvaluator {
  private static final MacroParserOptions OPTIONS = MacroParserOptions.builder()
    .setFunctionWhitelist(DefaultMacroEvaluator.SECURE_FUNCTION_NAME)
    .setEscaping(false)
    .build();
  private final SystemHttpServiceContext context;

  public SystemServicePropertyEvaluator(SystemHttpServiceContext context) {
    this.context = context;
  }

  @Override
  public Map<String, String> evaluate(String namespace, Map<String, String> properties) {
    MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(context.getRuntimeArguments(), context, namespace);
    return context.evaluateMacros(namespace, properties, macroEvaluator, OPTIONS);
  }
}
