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
import io.cdap.delta.api.ChangeEvent;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DMLEvent;

import java.lang.reflect.Type;

/**
 * Custom deserializer for a ChangeEvent, to create a DDLEvent or DMLEvent.
 */
class ChangeEventDeserializer implements JsonDeserializer<ChangeEvent> {

  @Override
  public ChangeEvent deserialize(JsonElement jsonElement, Type type,
                                 JsonDeserializationContext context) throws JsonParseException {
    JsonObject obj = jsonElement.getAsJsonObject();

    if (obj.has("ingestTimestampMillis")) {
      return context.deserialize(jsonElement, DMLEvent.class);
    }

    // DDL
    return context.deserialize(jsonElement, DDLEvent.class);
  }
}
