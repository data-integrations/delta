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

import com.google.common.io.ByteStreams;
import io.cdap.delta.api.DeltaPipelineId;
import io.cdap.delta.api.Offset;
import io.cdap.delta.app.DeltaWorkerId;
import io.cdap.delta.app.OffsetAndSequence;
import io.cdap.delta.app.service.common.AbstractAssessorHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Stores replicator state based on hadoop filesystem .
 * Is used only during migration
 */
public class HCFSStateStore implements StateStore {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractAssessorHandler.class);
  private static final String OFFSET_KEY = "offset";
  private final FileSystem fileSystem;
  private final Path basePath;

  private HCFSStateStore(FileSystem fileSystem, Path basePath) {
    this.fileSystem = fileSystem;
    this.basePath = basePath;
  }

  public static HCFSStateStore from(Path basePath) throws IOException {
    return new HCFSStateStore(basePath.getFileSystem(new Configuration()), basePath);
  }

  @Nullable
  public OffsetAndSequence readOffset(DeltaWorkerId id) throws IOException {
    Path path = getPath(id, OFFSET_KEY);
    if (!fileSystem.exists(path)) {
      return null;
    }
    try (FSDataInputStream inputStream = fileSystem.open(path)) {
      Map<String, String> offset = new HashMap<>();
      long sequenceNumber = inputStream.readLong();
      int numKeys = inputStream.readInt();
      for (int i = 0; i < numKeys; i++) {
        String key = inputStream.readUTF();
        String val = inputStream.readUTF();
        offset.put(key, val);
      }
      return new OffsetAndSequence(new Offset(offset), sequenceNumber);
    }
  }

  public void writeOffset(DeltaWorkerId id, OffsetAndSequence offset) throws IOException {
    Path path = getPath(id, OFFSET_KEY);
    try (FSDataOutputStream outputStream = fileSystem.create(path, true)) {
      outputStream.writeLong(offset.getSequenceNumber());
      outputStream.writeInt(offset.getOffset().get().size());
      for (Map.Entry<String, String> entry : offset.getOffset().get().entrySet()) {
        outputStream.writeUTF(entry.getKey());
        outputStream.writeUTF(entry.getValue());
      }
    }
  }

  /**
   * @return the most recent generation of the given pipeline
   */
  @Nullable
  public Long getLatestGeneration(String namespace, String pipelineName) throws IOException {
    Path path = new Path(basePath, String.format("%s/%s", namespace, pipelineName));
    if (!fileSystem.exists(path)) {
      return null;
    }
    long maxGeneration = -1L;
    for (FileStatus file : fileSystem.listStatus(path)) {
      long gen = Long.parseLong(file.getPath().getName());
      maxGeneration = Math.max(gen, maxGeneration);
    }
    return maxGeneration > 0 ? maxGeneration : null;
  }

  public Collection<Integer> getWorkerInstances(DeltaPipelineId pipelineId) throws IOException {
    Path path = new Path(basePath, String.format("%s/%s/%d", pipelineId.getNamespace(), pipelineId.getApp(),
                                                 pipelineId.getGeneration()));
    if (!fileSystem.exists(path)) {
      return Collections.emptyList();
    }
    FileStatus[] files = fileSystem.listStatus(path);
    List<Integer> workerInstances = new ArrayList<>(files.length);
    for (FileStatus file : files) {
      // everything in here should be directories corresponding to an instance id unless somebody manually
      // modified it
      if (file.isDirectory()) {
        String fileName = file.getPath().getName();
        try {
          workerInstances.add(Integer.parseInt(fileName));
        } catch (NumberFormatException e) {
          // should not happen unless somebody manually modified the directories
          LOG.error(String.format("Worker instance '%s' isn't valid.", fileName), e);
        }
      }
    }
    return workerInstances;
  }

  @Nullable
  public byte[] readState(DeltaWorkerId id, String key) throws IOException {
    Path path = getPath(id, key);
    if (!fileSystem.exists(path)) {
      return new byte[0];
    }
    try (FSDataInputStream inputStream = fileSystem.open(path)) {
      return ByteStreams.toByteArray(inputStream);
    }
  }

  public void writeState(DeltaWorkerId id, String key, byte[] val) throws IOException {
    Path path = getPath(id, key);
    try (FSDataOutputStream outputStream = fileSystem.create(path, true)) {
      outputStream.write(val);
    }
  }

  private Path getPath(DeltaWorkerId id, String key) {
    return new Path(basePath, String.format("%s/%s/%d/%d/%s",
                                            id.getPipelineId().getNamespace(), id.getPipelineId().getApp(),
                                            id.getPipelineId().getGeneration(), id.getInstanceId(), key));
  }
}
