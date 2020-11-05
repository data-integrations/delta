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
import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaTarget;
import io.cdap.delta.api.SourceTable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
  private final List<SourceTable> tables;
  private final Set<DMLOperation.Type> dmlBlacklist;
  private final Set<DDLOperation.Type> ddlBlacklist;
  private final RetryConfig retries;
  private final ParallelismConfig parallelism;
  // should only be set by CDAP admin when creating the system service
  private final boolean service;

  private DeltaConfig(String description, List<Stage> stages, List<Connection> connections,
                      Resources resources, String offsetBasePath, List<SourceTable> tables,
                      Set<DMLOperation.Type> dmlBlacklist, Set<DDLOperation.Type> ddlBlacklist,
                      RetryConfig retries, ParallelismConfig parallelism) {
    this.description = description;
    this.stages = new ArrayList<>(stages);
    this.connections = new ArrayList<>(connections);
    this.resources = resources;
    this.offsetBasePath = offsetBasePath;
    this.tables = new ArrayList<>(tables);
    this.service = false;
    this.dmlBlacklist = new HashSet<>(dmlBlacklist);
    this.ddlBlacklist = new HashSet<>(ddlBlacklist);
    this.retries = retries;
    this.parallelism = parallelism;
  }

  @Nullable
  public String getDescription() {
    return description;
  }

  public List<Stage> getStages() {
    // can be null when this class is created through deserialization of user input
    return stages == null ? Collections.emptyList() : Collections.unmodifiableList(stages);
  }

  public String getOffsetBasePath() {
    return offsetBasePath == null || offsetBasePath.isEmpty() ? "cdap/delta/" : offsetBasePath;
  }

  public List<Connection> getConnections() {
    return connections == null ? Collections.emptyList() : Collections.unmodifiableList(connections);
  }

  public Resources getResources() {
    return resources == null ? new Resources(8192, 4) : resources;
  }

  public List<SourceTable> getTables() {
    return tables == null ? Collections.emptyList() : Collections.unmodifiableList(tables);
  }

  public Set<DMLOperation.Type> getDmlBlacklist() {
    return dmlBlacklist == null ? Collections.emptySet() : Collections.unmodifiableSet(dmlBlacklist);
  }

  public Set<DDLOperation.Type> getDdlBlacklist() {
    // blacklist drop database event by default if ddlBlacklist is null
    return ddlBlacklist == null ? Collections.singleton(DDLOperation.Type.DROP_DATABASE) :
      Collections.unmodifiableSet(ddlBlacklist);
  }

  /**
   * @return number of minutes to retry failures before failing the program run. A non-positive number means there
   *   is no retry limit.
   */
  public RetryConfig getRetryConfig() {
    return retries == null ? RetryConfig.DEFAULT : retries;
  }

  public ParallelismConfig getParallelism() {
    return parallelism == null ? ParallelismConfig.DEFAULT : parallelism;
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
  }

  public Stage getSource() {
    return getStages().stream()
      .filter(s -> DeltaSource.PLUGIN_TYPE.equals(s.getPlugin().getType()))
      .findFirst()
      .orElseThrow(() -> new IllegalArgumentException("No source stage found."));
  }

  @Nullable
  public Stage getTarget() {
    return getStages().stream()
      .filter(s -> DeltaTarget.PLUGIN_TYPE.equals(s.getPlugin().getType()))
      .findFirst()
      .orElse(null);
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
    DeltaConfig that = (DeltaConfig) o;
    return service == that.service &&
      Objects.equals(description, that.description) &&
      Objects.equals(stages, that.stages) &&
      Objects.equals(connections, that.connections) &&
      Objects.equals(resources, that.resources) &&
      Objects.equals(offsetBasePath, that.offsetBasePath) &&
      Objects.equals(tables, that.tables) &&
      Objects.equals(dmlBlacklist, that.dmlBlacklist) &&
      Objects.equals(ddlBlacklist, that.ddlBlacklist);
  }

  @Override
  public int hashCode() {
    return Objects.hash(description, stages, connections, resources, offsetBasePath, tables,
                        dmlBlacklist, ddlBlacklist, service);
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builds a DeltaConfig.
   */
  public static class Builder {
    private Stage source;
    private Stage target;
    private String description;
    private String offsetBasePath;
    private Resources resources;
    private List<SourceTable> tables;
    private Set<DMLOperation.Type> dmlBlacklist;
    private Set<DDLOperation.Type> ddlBlacklist;
    private RetryConfig retries;
    private ParallelismConfig parallelism;

    private Builder() {
      description = "";
      resources = new Resources();
      tables = new ArrayList<>();
      dmlBlacklist = new HashSet<>();
      ddlBlacklist = new HashSet<>();
      retries = RetryConfig.DEFAULT;
      parallelism = ParallelismConfig.DEFAULT;
    }

    public Builder setSource(Stage source) {
      this.source = source;
      return this;
    }

    public Builder setTarget(Stage target) {
      this.target = target;
      return this;
    }

    public Builder setDescription(String description) {
      this.description = description;
      return this;
    }

    public Builder setOffsetBasePath(String offsetBasePath) {
      this.offsetBasePath = offsetBasePath;
      return this;
    }

    public Builder setResources(Resources resources) {
      this.resources = resources;
      return this;
    }

    public Builder setTables(Collection<SourceTable> tables) {
      this.tables.clear();
      this.tables.addAll(tables);
      return this;
    }

    public Builder setDMLBlacklist(Collection<DMLOperation.Type> blacklist) {
      this.dmlBlacklist.clear();
      this.dmlBlacklist.addAll(blacklist);
      return this;
    }

    public Builder setDDLBlacklist(Collection<DDLOperation.Type> blacklist) {
      this.ddlBlacklist.clear();
      this.ddlBlacklist.addAll(blacklist);
      return this;
    }

    public Builder setRetryConfig(RetryConfig retryConfig) {
      this.retries = retryConfig;
      return this;
    }

    public Builder setParallelism(ParallelismConfig parallelism) {
      this.parallelism = parallelism;
      return this;
    }

    public DeltaConfig build() {
      List<Stage> stages = new ArrayList<>();
      if (source != null) {
        stages.add(source);
      }
      if (target != null) {
        stages.add(target);
      }
      List<Connection> connections = new ArrayList<>();
      if (source != null && target != null) {
        connections.add(new Connection(source.getName(), target.getName()));
      }
      DeltaConfig config = new DeltaConfig(description, stages, connections, resources, offsetBasePath, tables,
                                           dmlBlacklist, ddlBlacklist, retries, parallelism);
      config.validate();
      return config;
    }
  }
}
