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

package io.cdap.delta.app;

import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.EventConsumer;
import io.cdap.delta.api.Sequenced;

import java.util.ArrayList;
import java.util.List;

/**
 * Keeps track of the events it got, in the order it got them.
 */
public class TrackingEventConsumer implements EventConsumer {
  private final List<Sequenced<DMLEvent>> dmlEvents = new ArrayList<>();
  private final List<Sequenced<DDLEvent>> ddlEvents = new ArrayList<>();

  @Override
  public void start() {
    // no-op
  }

  @Override
  public void stop() {
    // no-op
  }

  @Override
  public void applyDDL(Sequenced<DDLEvent> event) {
    ddlEvents.add(event);
  }

  @Override
  public void applyDML(Sequenced<DMLEvent> event) {
    dmlEvents.add(event);
  }

  public List<Sequenced<DMLEvent>> getDmlEvents() {
    return dmlEvents;
  }

  public List<Sequenced<DDLEvent>> getDdlEvents() {
    return ddlEvents;
  }
}