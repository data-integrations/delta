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

import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.EventReaderDefinition;
import io.cdap.delta.api.assessment.TableAssessor;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableRegistry;


/**
 * Mock source that returns pre-determined table list and table detail.
 */
public class MockSource implements DeltaSource {
  private final TableRegistry registry;
  private final TableAssessor<TableDetail> assessor;

  public MockSource(TableRegistry registry, TableAssessor<TableDetail> assessor) {
    this.registry = registry;
    this.assessor = assessor;
  }

  @Override
  public void configure(Configurer configurer) {
    // no-op
  }

  @Override
  public EventReader createReader(EventReaderDefinition definition, DeltaSourceContext context, EventEmitter emitter) {
    return null;
  }

  @Override
  public TableRegistry createTableRegistry(Configurer configurer) {
    return registry;
  }

  @Override
  public TableAssessor<TableDetail> createTableAssessor(Configurer configurer) {
    return assessor;
  }
}
