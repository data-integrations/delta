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

import com.google.common.io.ByteStreams;
import io.cdap.delta.api.Offset;
import io.cdap.delta.app.DeltaPipelineId;
import io.cdap.delta.app.OffsetAndSequence;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Stores replicator state.
 */
public class StateStore {
  private static final String STATE_PREFIX = "state-";
  private static final String OFFSET_KEY = "offset";
  private final FileSystem fileSystem;
  private final Path basePath;

  public StateStore(FileSystem fileSystem, Path basePath) {
    this.fileSystem = fileSystem;
    this.basePath = basePath;
  }

  @Nullable
  public OffsetAndSequence readOffset(DeltaPipelineId id) throws IOException {
    Path path = getPath(id, OFFSET_KEY);
    if (!fileSystem.exists(path)) {
      return null;
    }
    try (FSDataInputStream inputStream = fileSystem.open(path)) {
      Map<String, byte[]> offset = new HashMap<>();
      long sequenceNumber = inputStream.readLong();
      int numKeys = inputStream.readInt();
      for (int i = 0; i < numKeys; i++) {
        String key = inputStream.readUTF();
        int len = inputStream.readInt();
        byte[] val = new byte[len];
        inputStream.readFully(val);
        offset.put(key, val);
      }
      return new OffsetAndSequence(new Offset(offset), sequenceNumber);
    }
  }

  public void writeOffset(DeltaPipelineId id, OffsetAndSequence offset) throws IOException {
    Path path = getPath(id, OFFSET_KEY);
    try (FSDataOutputStream outputStream = fileSystem.create(path, true)) {
      outputStream.writeLong(offset.getSequenceNumber());
      outputStream.writeInt(offset.getOffset().get().size());
      for (Map.Entry<String, byte[]> entry : offset.getOffset().get().entrySet()) {
        outputStream.writeUTF(entry.getKey());
        outputStream.writeInt(entry.getValue().length);
        outputStream.write(entry.getValue());
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

  @Nullable
  public byte[] readState(DeltaPipelineId id, String key) throws IOException {
    Path path = getPath(id, STATE_PREFIX + key);
    if (!fileSystem.exists(path)) {
      return null;
    }
    try (FSDataInputStream inputStream = fileSystem.open(path)) {
      return ByteStreams.toByteArray(inputStream);
    }
  }

  public void writeState(DeltaPipelineId id, String key, byte[] val) throws IOException {
    Path path = getPath(id, STATE_PREFIX + key);
    try (FSDataOutputStream outputStream = fileSystem.create(path, true)) {
      outputStream.write(val);
    }
  }

  private Path getPath(DeltaPipelineId id, String key) {
    return new Path(basePath, String.format("%s/%s/%s/%s",
                                            id.getNamespace(), id.getApp(), id.getGeneration(), key));
  }
}
