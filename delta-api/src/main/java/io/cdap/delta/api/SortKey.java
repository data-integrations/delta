/*
 *
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.delta.api;

import io.cdap.cdap.api.data.schema.Schema;

/**
 * Used to store type and value of a sort key
 * List of sort keys is used for ordering DML events
 */
public class SortKey {
    private final Schema.Type type;
    private final Object value;

    public SortKey(Schema.Type type, Object value) {
        this.type = type;
        this.value = value;
    }

    public Schema.Type getType() {
        return type;
    }

    public <T> T getValue() {
        return (T) value;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SortKey{");
        sb.append("type=").append(type);
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }
}
