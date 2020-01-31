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

package io.cdap.delta.store;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.security.store.SecureStore;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Evaluates macros in the app.
 */
public class DefaultMacroEvaluator implements MacroEvaluator {
  public static final String SECURE_FUNCTION_NAME = "secure";
  private final Map<String, String> runtimeArgs;
  private final SecureStore secureStore;
  private final String namespace;

  public DefaultMacroEvaluator(Map<String, String> runtimeArgs, SecureStore secureStore, String namespace) {
    this.runtimeArgs = new HashMap<>(runtimeArgs);
    this.secureStore = secureStore;
    this.namespace = namespace;
  }

  @Override
  @Nullable
  public String lookup(String property) {
    String val = runtimeArgs.get(property);
    if (val == null) {
      throw new InvalidMacroException(String.format("Argument '%s' is not defined.", property));
    }
    return val;
  }

  @Override
  @Nullable
  public String evaluate(String macroFunction, String... arguments) throws InvalidMacroException {
    if (macroFunction.equals(SECURE_FUNCTION_NAME)) {
      if (arguments.length != 1) {
        throw new InvalidMacroException("Secure store macro function only supports 1 argument.");
      }
      try {
        return Bytes.toString(secureStore.get(namespace, arguments[0]).get());
      } catch (Exception e) {
        throw new InvalidMacroException("Failed to resolve macro '" + macroFunction + "(" + arguments[0] + ")'", e);
      }
    }
    throw new InvalidMacroException(String.format("Unsupported macro function %s", macroFunction));
  }
}
