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

import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.Resources;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaTarget;
import io.cdap.delta.api.SourceTable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Config for a Delta app.
 * Null checks in the getters are done since this object is normally created by GSON deserialization of user input.
 */
public class DeltaConfig extends Config {
  private final String description;
  private final List<Stage> stages;
  private final List<Connection> connections;
  private final Resources resources;
  private final String offsetBasePath;
  private final boolean service;
  private final List<SourceTable> tables;

  public DeltaConfig(Stage source, Stage target, List<SourceTable> tables) {
    this(source, target, tables, null, null);
  }

  public DeltaConfig(Stage source, Stage target, List<SourceTable> tables,
                     Resources resources, String offsetBasePath) {
    this.description = "";
    this.stages = Collections.unmodifiableList(Arrays.asList(source, target));
    this.connections = Collections.singletonList(new Connection(source.getName(), target.getName()));
    this.tables = Collections.unmodifiableList(new ArrayList<>(tables));
    this.resources = resources;
    this.offsetBasePath = offsetBasePath;
    this.service = false;
  }

  @Nullable
  public String getDescription() {
    return description;
  }

  public List<Stage> getStages() {
    return stages == null ? Collections.emptyList() : stages;
  }

  public String getOffsetBasePath() {
    return offsetBasePath == null || offsetBasePath.isEmpty() ? "cdap/delta/" : offsetBasePath;
  }

  public List<Connection> getConnections() {
    return connections == null ? Collections.emptyList() : connections;
  }

  public Resources getResources() {
    return resources == null ? new Resources(8192, 4) : resources;
  }

  public List<SourceTable> getTables() {
    return tables == null ? Collections.emptyList() : tables;
  }

  public boolean isService() {
    return service;
  }

  /**
   * Validate that the config is a valid draft. A valid draft must contain a single valid source stage.
   * If it contains a target, the target must also be valid.
   */
  public void validateDraft() {
    validate();
  }

  /**
   * Validate that the config is a valid pipeline. A valid pipeline must contain a single valid source stage
   * and a single valid target stage.
   */
  public void validatePipeline() {
    validate().orElseThrow(() -> new IllegalArgumentException("A target must be specified."));
    if (getTables().isEmpty()) {
      throw new IllegalArgumentException("At least one source table must be configured.");
    }
  }

  public Stage getSource() {
    return getStages().stream()
      .filter(s -> DeltaSource.PLUGIN_TYPE.equals(s.getPlugin().getType()))
      .findFirst()
      .orElseThrow(() -> new IllegalArgumentException("No source stage found."));
  }

  public Stage getTarget() {
    return getStages().stream()
      .filter(s -> DeltaTarget.PLUGIN_TYPE.equals(s.getPlugin().getType()))
      .findFirst()
      .orElseThrow(() -> new IllegalArgumentException("No target stage found."));
  }

  /**
   * Validate that the stages contain all required fields and that there is a source defined.
   *
   * @return the target stage if it exists
   */
  private Optional<Stage> validate() {
    Stage sourceStage = null;
    Stage targetStage = null;
    for (Stage stage : stages) {
      stage.validate();
      if (DeltaSource.PLUGIN_TYPE.equals(stage.getPlugin().getType())) {
        if (sourceStage != null) {
          throw new IllegalArgumentException(
            String.format("Pipeline can only have one source, but '%s' and '%s' are both sources.",
                          sourceStage.getName(), stage.getName()));
        }
        sourceStage = stage;
      }
      if (DeltaTarget.PLUGIN_TYPE.equals(stage.getPlugin().getType())) {
        if (targetStage != null) {
          throw new IllegalArgumentException(
            String.format("Pipeline can only have one target, but '%s' and '%s' are both targets.",
                          targetStage.getName(), stage.getName()));
        }
        targetStage = stage;
      }
    }
    if (sourceStage == null) {
      throw new IllegalArgumentException("No source found.");
    }
    return Optional.ofNullable(targetStage);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeltaConfig config = (DeltaConfig) o;
    return service == config.service &&
      Objects.equals(description, config.description) &&
      Objects.equals(stages, config.stages) &&
      Objects.equals(connections, config.connections) &&
      Objects.equals(resources, config.resources) &&
      Objects.equals(offsetBasePath, config.offsetBasePath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(description, stages, connections, resources, offsetBasePath, service);
  }
}
