/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.delta.mysql;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReader;

/**
 * Mysql origin.
 */
@Plugin(type = DeltaSource.PLUGIN_TYPE)
@Name(MySqlDeltaSource.NAME)
@Description("CDC origin for MySQL.")
public class MySqlDeltaSource implements DeltaSource {
  public static final String NAME = "mysql";
  private final MySqlConfig conf;

  public MySqlDeltaSource(MySqlConfig conf) {
    this.conf = conf;
  }

  @Override
  public void configure(Configurer configurer) {
    // no-op
  }

  @Override
  public EventReader createReader(DeltaSourceContext context, EventEmitter eventEmitter) {
    return new MySqlEventReader(conf, context, eventEmitter);
  }
}
