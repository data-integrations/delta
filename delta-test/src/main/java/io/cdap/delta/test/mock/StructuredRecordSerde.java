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

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.StructuredRecordStringConverter;

import java.io.IOException;
import java.lang.reflect.Type;

/**
 * Serde for StructuredRecord. Required because Gson can't be used to directly serialize a StructuredRecord, due to
 * the fact that it has a Map of Objects. Serializing it directly can convert ints to doubles and things like that.
 */
public class StructuredRecordSerde implements JsonSerializer<StructuredRecord>, JsonDeserializer<StructuredRecord> {

  @Override
  public StructuredRecord deserialize(JsonElement jsonElement, Type type,
                                      JsonDeserializationContext context) throws JsonParseException {
    JsonObject obj = jsonElement.getAsJsonObject();
    Schema schema = context.deserialize(obj.get("schema"), Schema.class);
    try {
      return StructuredRecordStringConverter.fromJsonString(obj.get("record").getAsString(), schema);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public JsonElement serialize(StructuredRecord structuredRecord, Type type,
                               JsonSerializationContext context) {
    JsonObject obj = new JsonObject();
    obj.add("schema", context.serialize(structuredRecord.getSchema(), Schema.class));
    try {
      obj.addProperty("record", StructuredRecordStringConverter.toJsonString(structuredRecord));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return obj;
  }
}
