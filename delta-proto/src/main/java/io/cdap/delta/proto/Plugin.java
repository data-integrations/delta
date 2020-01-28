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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a plugin.
 * Null checks in the getters are done since this object is normally created by GSON deserialization of user input.
 */
public class Plugin {
  private final String name;
  private final String type;
  private final Map<String, String> properties;
  private final Artifact artifact;

  public Plugin(String name, String type, Map<String, String> properties, Artifact artifact) {
    this.name = name;
    this.type = type;
    this.properties = properties;
    this.artifact = artifact;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public Map<String, String> getProperties() {
    return properties == null ? Collections.emptyMap() : properties;
  }

  public Artifact getArtifact() {
    return artifact == null ? Artifact.EMPTY : artifact;
  }

  /**
   * Validate that the plugin config is valid, throwing an {@link IllegalArgumentException} if not.
   */
  public void validate() {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("Plugin name is missing.");
    }
    if (type == null || type.isEmpty()) {
      throw new IllegalArgumentException("Plugin type is missing.");
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
    Plugin plugin = (Plugin) o;
    return Objects.equals(name, plugin.name) &&
      Objects.equals(type, plugin.type) &&
      Objects.equals(properties, plugin.properties) &&
      Objects.equals(artifact, plugin.artifact);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, properties, artifact);
  }
}
