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

import io.cdap.cdap.api.app.ApplicationConfigUpdateAction;
import io.cdap.cdap.api.app.ApplicationUpdateContext;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactVersionRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Represent a stage in the delta pipeline.
 */
public class Stage {
  private static final Logger LOG = LoggerFactory.getLogger(Stage.class);
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
   * Updates stage by performing update action logic provided in context.
   * Current relevant update actions for stages are:
   *  1. UPGRADE_ARTIFACT: Upgrades plugin artifact by finding the latest version of plugin to use.
   *
   * @param updateContext Context to use for updating stage.
   * @return new (updated) Stage.
   */
  public Stage updateStage(ApplicationUpdateContext updateContext) throws Exception {
    for (ApplicationConfigUpdateAction updateAction : updateContext.getUpdateActions()) {
      if (updateAction == ApplicationConfigUpdateAction.UPGRADE_ARTIFACT) {
        return new Stage(name, upgradePlugin(updateContext));
      }
      return this;
    }

    // No update action provided so return stage as is.
    return this;
  }

  /**
   * Upgrade plugin used in the stage.
   * 1. If plugin is using fixed version and a new plugin artifact is found with higher version in SYSTEM scope,
   *    use the new plugin.
   * 2. If plugin is using a plugin range and a new plugin artifact is found with higher version in SYSTEM scope,
   *    move the upper bound of the range to include the new plugin artifact. Also change plugin scope.
   *    If new plugin is in range, do not change range. (Note: It would not change range even though new plugin is in
   *    different scope).
   *
   * @param updateContext To use helper functions like getPluginArtifacts.
   * @return Updated plugin object to be used for the udated stage. Returned null if no changes to current plugin.
   */
  private Plugin upgradePlugin(ApplicationUpdateContext updateContext) throws Exception {
    // Find the plugin with max version from available candidates.
    Optional<ArtifactId> newPluginCandidate =
      updateContext.getPluginArtifacts(plugin.getType(), plugin.getName(), null).stream()
        .max(Comparator.comparing(ArtifactId::getVersion));
    if (!newPluginCandidate.isPresent()) {
      // This should not happen as there should be at least one plugin candidate same as current.
      // TODO: Consider throwing exception here.
      return plugin;
    }

    ArtifactId newPlugin = newPluginCandidate.get();
    String newVersion = getUpgradedVersionString(newPlugin);
    // If getUpgradedVersionString returns null, candidate plugin is not valid for upgrade.
    if (newVersion == null) {
      return plugin;
    }

    Artifact newArtifact = new Artifact(newPlugin.getName(), newVersion, newPlugin.getScope().name());
    return new Plugin(plugin.getName(), plugin.getType(), plugin.getProperties(), newArtifact);
  }

  /**
   * Returns new valid version string for plugin upgrade if any changes are required. Returns null if no change to
   * current plugin version.
   * Artifact selector config only stores plugin version as string, it can be either fixed version or range.
   * Hence, if the plugin version is fixed, replace the fixed version with newer fixed version. If it is a range,
   * move the upper bound of the range to the newest version.
   *
   * @param newPlugin New candidate plugin for updating plugin artifact.
   * @return version string to be used for new plugin. Might be fixed version/version range string depending on
   *         current use.
   */
  @Nullable
  private String getUpgradedVersionString(ArtifactId newPlugin) {
    ArtifactVersionRange currentVersionRange;
    try {
      currentVersionRange = ArtifactVersionRange.parse(Objects.requireNonNull(plugin.getArtifact().getVersion()));
    } catch (Exception e) {
      LOG.warn("Issue in parsing version string for plugin {}, ignoring stage {} for upgrade.", plugin, name, e);
      return null;
    }

    if (currentVersionRange.isExactVersion()) {
      if (currentVersionRange.getLower().compareTo(newPlugin.getVersion()) < 0) {
        // Current version is a fixed version and new version is higher than current.
        return newPlugin.getVersion().getVersion();
      }
      return null;
    }

    // Current plugin version is version range.
    if (currentVersionRange.versionIsInRange(newPlugin.getVersion())) {
      // Do nothing and return as is. Note that plugin scope will not change.
      // TODO: Figure out how to change plugin scope if a newer plugin is found but in different scope.
      return null;
    }
    // Current lower version is higher than newer latest version. This should not happen.
    if (currentVersionRange.getLower().compareTo(newPlugin.getVersion()) > 0) {
      LOG.warn("Error in updating stage {}. Invalid new plugin artifact {} upgrading plugin {}.",
               name, newPlugin, plugin);
      return null;
    }
    // Increase the upper bound to latest available version.
    ArtifactVersionRange newVersionRange =
      new ArtifactVersionRange(currentVersionRange.getLower(), currentVersionRange.isLowerInclusive(),
                               newPlugin.getVersion(), true);
    return newVersionRange.getVersionString();
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
      throw new IllegalArgumentException(String.format("Stage '%s' is invalid: %s", name, e.getMessage()), e);
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
