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
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Configures the number of instances, or specify exactly which tables should be assigned to each instance.
 */
public class ParallelismConfig {
  public static final ParallelismConfig DEFAULT = new ParallelismConfig(1, Collections.emptyList());
  private final Integer numInstances;
  private final List<InstanceConfig> instances;

  public ParallelismConfig(int numInstances) {
    this(numInstances, Collections.emptyList());
  }

  public ParallelismConfig(List<InstanceConfig> instances) {
    this(null, instances);
  }

  private ParallelismConfig(Integer numInstances, List<InstanceConfig> instances) {
    this.numInstances = numInstances;
    this.instances = instances;
  }

  @Nullable
  public Integer getNumInstances() {
    return numInstances;
  }

  public List<InstanceConfig> getInstances() {
    return instances == null ? Collections.emptyList() : Collections.unmodifiableList(instances);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ParallelismConfig that = (ParallelismConfig) o;
    return numInstances == that.numInstances &&
      Objects.equals(getInstances(), that.getInstances());
  }

  @Override
  public int hashCode() {
    return Objects.hash(numInstances, instances);
  }
}
