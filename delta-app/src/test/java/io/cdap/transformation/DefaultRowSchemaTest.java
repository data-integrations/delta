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

package io.cdap.transformation;

import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

public class DefaultRowSchemaTest {


  @Test(expected = NullPointerException.class)
  public void testNullSchema() {
    new DefaultRowSchema(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalSchema() {
    new DefaultRowSchema(Schema.of(Schema.Type.STRING));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetNonExistingField() {
    DefaultRowSchema rowSchema = new DefaultRowSchema(
      Schema.recordOf("record", Schema.Field.of("strCol", Schema.of(Schema.Type.STRING))));
    rowSchema.getField("nonExisting");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetNullField() {
    DefaultRowSchema rowSchema = new DefaultRowSchema(
      Schema.recordOf("record", Schema.Field.of("strCol", Schema.of(Schema.Type.STRING))));
    rowSchema.getField(null);
  }

  @Test(expected = NullPointerException.class)
  public void testSetNullColumnName() {
    DefaultRowSchema rowSchema = new DefaultRowSchema(
      Schema.recordOf("record", Schema.Field.of("strCol", Schema.of(Schema.Type.STRING))));
    rowSchema.setField(null, Schema.Field.of("strCol", Schema.of(Schema.Type.STRING)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddInvalidFieldName() {
    DefaultRowSchema rowSchema = new DefaultRowSchema(
      Schema.recordOf("record", Schema.Field.of("strCol", Schema.of(Schema.Type.STRING))));
    rowSchema.setField("strCol1", Schema.Field.of("strCol2", Schema.of(Schema.Type.STRING)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRenameToExisting() {
    DefaultRowSchema rowSchema = new DefaultRowSchema(
      Schema.recordOf("record",
                      Schema.Field.of("strCol", Schema.of(Schema.Type.STRING)),
                      Schema.Field.of("intCol", Schema.of(Schema.Type.INT))));
    rowSchema.setField("strCol", Schema.Field.of("intCol", Schema.of(Schema.Type.STRING)));
  }

  @Test(expected = NullPointerException.class)
  public void testSetNullField() {
    DefaultRowSchema rowSchema = new DefaultRowSchema(
      Schema.recordOf("record", Schema.Field.of("strCol", Schema.of(Schema.Type.STRING))));
    rowSchema.setField("strCol", null);
  }

  @Test
  public void testValidSchema() {
    Schema.Field strField = Schema.Field.of("strCol", Schema.of(Schema.Type.STRING));
    Schema.Field intField = Schema.Field.of("intCol", Schema.of(Schema.Type.INT));
    Schema.Field boolField = Schema.Field.of("intCol", Schema.of(Schema.Type.BOOLEAN));
    Schema.Field strField1 = Schema.Field.of("strCol1", Schema.of(Schema.Type.STRING));
    DefaultRowSchema rowSchema = new DefaultRowSchema(
      Schema.recordOf("record",
                      strField,
                      intField));

    // get the original two columns
    Schema.Field field = rowSchema.getField("strCol");
    Assert.assertEquals(strField.getName(), field.getName());
    Assert.assertEquals(strField.getSchema(), field.getSchema());
    field = rowSchema.getField("intCol");
    Assert.assertEquals(intField.getName(), field.getName());
    Assert.assertEquals(intField.getSchema(), field.getSchema());

    //change the type
    rowSchema.setField("intCol", boolField);
    field = rowSchema.getField("intCol");
    Assert.assertEquals(boolField.getName(), field.getName());
    Assert.assertEquals(boolField.getSchema(), field.getSchema());

    //rename
    rowSchema.setField("strCol", strField1);
    field = rowSchema.getField("strCol1");
    Assert.assertEquals(strField1.getName(), field.getName());
    Assert.assertEquals(strField1.getSchema(), field.getSchema());
    Assert.assertEquals("strCol1", rowSchema.getRenameInfo().getNewName("strCol"));
  }

}
