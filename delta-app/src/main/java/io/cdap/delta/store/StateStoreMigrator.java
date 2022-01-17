/*
 * Copyright © 2021 Cask Data, Inc.
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

import io.cdap.delta.api.DeltaPipelineId;
import io.cdap.delta.app.DeltaWorkerId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A util class to transfer any state store data present on HCFS path by an old pipeline with HCF based state store
 * into SQL based state store on the first pipeline run after upgrade.
 */
public class StateStoreMigrator {

  private static final Logger LOG = LoggerFactory.getLogger(StateStoreMigrator.class);
  private final String hcfsStateStorePath;
  private final DeltaWorkerId currentId;
  private final StateStore remoteStateStore;

  private FileSystem fileSystem;
  private Path instanceBasePath;
  protected HCFSStateStore hcfsStateStore;
  protected DeltaWorkerId previousId;

  public StateStoreMigrator(String hcfsStateStorePath, DeltaWorkerId currentId, StateStore remoteStateStore) {
    this.hcfsStateStorePath = hcfsStateStorePath;
    this.currentId = currentId;
    this.remoteStateStore = remoteStateStore;
  }

  public boolean perform() throws IOException {
    //Check if path is not empty
    if (hcfsStateStorePath == null || hcfsStateStorePath.isEmpty()) {
      return false;
    }

    //Initiate HCFS state store to read
    Path path = new Path(hcfsStateStorePath);
    hcfsStateStore = HCFSStateStore.from(path);

    //Get previous latest generation : required as we cannot access previous context.
    Long previousGeneration = hcfsStateStore.getLatestGeneration(currentId.getPipelineId().getNamespace(),
                                                                 currentId.getPipelineId().getApp());
    if (previousGeneration == null) {
      LOG.debug("No generation number was found for the App in the hcfs path : {}", hcfsStateStorePath);
      return false;
    }

    previousId = new DeltaWorkerId(new DeltaPipelineId(currentId.getPipelineId().getNamespace(),
                          currentId.getPipelineId().getApp(), previousGeneration), currentId.getInstanceId());

    fileSystem = path.getFileSystem(new Configuration());
    //Base path for this worker
    instanceBasePath = new Path(path, String.format("%s/%s/%d/%d",
                                                    previousId.getPipelineId().getNamespace(),
                                                    previousId.getPipelineId().getApp(),
                                                    previousId.getPipelineId().getGeneration(),
                                                    previousId.getInstanceId()));

    if (!fileSystem.exists(instanceBasePath)) {
      return false;
    }

    FileStatus[] fStats = fileSystem.listStatus(instanceBasePath);
    if (fStats.length == 0) {
      return false;
    }

    for (FileStatus stateFile : fStats) {
      Path filePath = stateFile.getPath();

      if (filePath.getName().equals(RemoteStateStore.OFFSET_KEY)) {
        migrateOffset();
      } else {
        migrateStateFile(filePath.getName());
      }
    }

    fileSystem.rename(instanceBasePath, new Path(path, String.format("%s/%s/%d/%s",
                                                                     previousId.getPipelineId().getNamespace(),
                                                                     previousId.getPipelineId().getApp(),
                                                                     previousId.getPipelineId().getGeneration(),
                                                                     previousId.getInstanceId() + "_migrated")));
    return true;
  }

  private void migrateOffset() throws IOException {
    LOG.debug("Migrating Offset data from previous HCFS Location");
    if (remoteStateStore.readOffset(this.currentId) == null) {
      remoteStateStore.writeOffset(this.currentId, hcfsStateStore.readOffset(this.previousId));
    }
  }

  private void migrateStateFile(String key) throws IOException {
    LOG.debug("Migrating {} state data from previous HCFS Location", key);
    byte[] presentDBStateData = remoteStateStore.readState(this.currentId, key);
    if (presentDBStateData == null || presentDBStateData.length == 0) {
      byte[] stateData = hcfsStateStore.readState(this.previousId, key);
      remoteStateStore.writeState(this.currentId, key, stateData);
    }
  }
}
