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

import java.util.Map;

/**
 * Evaluates macros in a map of properties.
 */
public interface PropertyEvaluator {

  /**
   * Evaluate any macros in the properties.
   *
   * @param namespace namespace that the evaluation is taking place in
   * @param properties properties to evaluate
   * @return evaluated properties
   */
  Map<String, String> evaluate(String namespace, Map<String, String> properties);
}
