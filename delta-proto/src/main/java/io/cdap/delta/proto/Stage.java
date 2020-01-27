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

import java.util.Objects;

/**
 * Represent a stage in the delta pipeline.
 */
public class Stage {
  private final String name;
  private final Plugin plugin;

  public Stage(String name, Plugin plugin) {
    this.name = name;
    this.plugin = plugin;
  }

  public String getName() {
    return name;
  }

  public Plugin getPlugin() {
    return plugin;
  }

  /**
   * Validate that the stage contains all required information, throwing a {@link IllegalArgumentException} if not.
   */
  public void validate() {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("Stage is missing a name.");
    }
    try {
      plugin.validate();
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format("Stage '%s' is invalid: %s", name, e.getMessage()));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Stage stage = (Stage) o;
    return Objects.equals(name, stage.name) &&
      Objects.equals(plugin, stage.plugin);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, plugin);
  }
}
