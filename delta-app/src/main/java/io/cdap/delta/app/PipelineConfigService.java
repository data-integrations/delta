/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.delta.app;

import java.util.Map;

/**
 * Service to resolve pipeline runtime configuration
 * Currently uses static runtime arguments, but can be enhanced
 * to fetch dynamic runtime configuration from CDF instance
 */
public class PipelineConfigService {

  public static final String DIAGNOSTIC_MODE_ENABLED = "diagnostic.mode.enabled";
  public static final String AGGREGATE_STATS_FREQUENCY_ARG = "aggregate.stats.frequency.sec";
  static final String DEFAULT_AGGR_STATS_INTERVAL_SEC = "600"; // 10 min

  private final boolean diagnosticModeEnabled;

  private final int logAggregateStatsIntervalSec;

  public PipelineConfigService(Map<String, String> runtimeArguments) {

    this.logAggregateStatsIntervalSec = Integer.parseInt(
      runtimeArguments.getOrDefault(AGGREGATE_STATS_FREQUENCY_ARG, DEFAULT_AGGR_STATS_INTERVAL_SEC));

    this.diagnosticModeEnabled = Boolean.parseBoolean(
      runtimeArguments.getOrDefault(DIAGNOSTIC_MODE_ENABLED, "false"));
  }

  public boolean isDiagnosticModeEnabled() {
    return diagnosticModeEnabled;
  }

  public int getLogAggregateStatsIntervalSec() {
    return logAggregateStatsIntervalSec;
  }
}
