/*
 *  Copyright © 2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.delta.protos;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Instance class
 */
public class Instance {
  private final String namespace;
  private final String name;
  private final String id;
  private final long created;
  private final long updated;
  private final String description;
  private final Map<String, Object> properties;

  public Instance(String namespace, String name, String id, long created, long updated, String description, Map<String,
          Object> properties) {
    this.namespace = namespace;
    this.name = name;
    this.id = id;
    this.created = created;
    this.updated = updated;
    this.description = description;
    this.properties = properties;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getName() {
    return name;
  }

  public String getId() {
    return id;
  }

  public long getCreated() {
    return created;
  }

  public long getUpdated() {
    return updated;
  }

  public String getDescription() {
    return description;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Instance instance = (Instance) o;
    return created == instance.created &&
            updated == instance.updated &&
            namespace.equals(instance.namespace) &&
            id.equals(instance.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, id, created, updated);
  }

  @Override
  public String toString() {
    return "Instance{" +
            "namespace='" + namespace + '\'' +
            ", name='" + name + '\'' +
            ", id='" + id + '\'' +
            ", created=" + created +
            ", updated=" + updated +
            ", description='" + description + '\'' +
            ", properties=" + properties +
            '}';
  }

  public static Builder builder(String id) {
    return new Builder(id);
  }

  public static Builder builder(String id, Instance instance) {
    return new Builder(id)
            .setCreated(instance.getCreated())
            .setNamespace(instance.getNamespace())
            .setName(instance.getName())
            .setDescription(instance.getDescription())
            .setProperties(instance.getProperties());
  }


  /**
   * Instance builder
   */
  public static class Builder {
    private final String id;
    private long created = -1L;
    private long updated = -1L;
    private String namespace;
    private String name;
    private String description;
    private Map<String, Object> properties = new HashMap<>();

    public Builder(String id) {
      this.id = id;
    }

    public Builder setCreated(long created) {
      this.created = created;
      return this;
    }

    public Builder setUpdated(long updated) {
      this.updated = updated;
      return this;
    }

    public Builder setNamespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setDescription(String description) {
      this.description = description;
      return this;
    }

    public Builder setProperties(Map<String, Object> properties) {
      this.properties.clear();
      this.properties.putAll(properties);
      return this;
    }

    public Builder putProperty(String key, String val) {
      this.properties.put(key, val);
      return this;
    }

    public Instance build() {
      return new Instance(namespace, name, id, created, updated, description, properties);
    }
  }
}
