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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

/**
 * Used to capture and log diagnostic information for a delta worker
 */
public class Diagnostics {
  private static final Logger LOG = LoggerFactory.getLogger(Diagnostics.class);

  public static void logDiagnosticInfo() {
    logThreadDump();
  }

  private static void logThreadDump() {
    try {
      ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
      ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);

      for (ThreadInfo threadInfo : threadInfos) {
        LOG.info("" + threadInfo);
      }
    } catch (Exception e) {
      LOG.warn("Error in capturing diagnostic thread dump", e);
    }
  }
}
