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

package io.cdap.delta.test.mock;

import io.cdap.delta.api.assessment.TableAssessment;
import io.cdap.delta.api.assessment.TableAssessor;
import org.junit.Assert;

/**
 * Mock assessor that just returns a pre-defined TableAssessment.
 *
 * @param <T> type of schema
 */
public class MockTableAssessor<T> implements TableAssessor<T> {
  private final TableAssessment assessment;
  private final T expectedTableDescriptor;

  public MockTableAssessor(TableAssessment assessment) {
    this(assessment, null);
  }

  public MockTableAssessor(TableAssessment assessment, T expectedTableDescriptor) {
    this.expectedTableDescriptor = expectedTableDescriptor;
    this.assessment = assessment;
  }

  @Override
  public TableAssessment assess(T tableDescriptor) {
    if (expectedTableDescriptor != null) {
      Assert.assertEquals(expectedTableDescriptor, tableDescriptor);
    }
    return assessment;
  }
}
