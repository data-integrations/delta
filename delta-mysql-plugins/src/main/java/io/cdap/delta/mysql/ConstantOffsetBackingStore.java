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

import com.google.gson.Gson;
import io.cdap.cdap.api.common.Bytes;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Returns an offset based on a configuration setting and doesn't actually store anything.
 * This is because the Delta app needs to have control over when offsets are stored and not Debezium.
 *
 *
 * OffsetBackingStore is pretty weird... keys are ByteBuffer representations of Strings like:
 *
 * {"schema":null,"payload":["delta",{"server":"dummy"}]}
 *
 * and keys are ByteBuffer representations of Strings like:
 *
 * {"file":"mysql-bin.000003","pos":16838027,"row":1,"server_id":223344,"event":2,"ts_sec":1234567890}
 */
public class ConstantOffsetBackingStore extends MemoryOffsetBackingStore {
  private static final Gson GSON = new Gson();
  private static final String KEY = "{\"schema\":null,\"payload\":[\"delta\",{\"server\":\"dummy\"}]}";

  @Override
  public void configure(WorkerConfig config) {
    // TODO: remove hack once EmbeddedEngine is no longer used

    String offsetStr = config.getString("offset.storage.file.filename");
    if ("|".equals(offsetStr) || offsetStr.isEmpty()) {
      return;
    }

    String[] offsetParts = offsetStr.split("\\|");
    String posStr = offsetParts[0];
    String fileStr = offsetParts[1];

    if (posStr.isEmpty() || fileStr.isEmpty()) {
      return;
    }
    Map<String, Object> offset = new HashMap<>();
    offset.put("file", fileStr);
    offset.put("pos", Long.parseLong(posStr));
    byte[] offsetBytes = Bytes.toBytes(GSON.toJson(offset));

    data.put(ByteBuffer.wrap(Bytes.toBytes(KEY)), ByteBuffer.wrap(offsetBytes));
  }
}
