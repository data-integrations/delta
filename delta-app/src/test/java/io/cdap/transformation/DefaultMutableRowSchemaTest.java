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
import io.cdap.transformation.api.NotFoundException;
import org.junit.Assert;
import org.junit.Test;

public class DefaultMutableRowSchemaTest {


  @Test(expected = IllegalArgumentException.class)
  public void testNullSchema() {
    new DefaultMutableRowSchema(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalSchema() {
    new DefaultMutableRowSchema(Schema.of(Schema.Type.STRING));
  }

  @Test(expected = NotFoundException.class)
  public void testGetNonExistingField() throws Exception {
    DefaultMutableRowSchema rowSchema = new DefaultMutableRowSchema(
      Schema.recordOf("record", Schema.Field.of("strCol", Schema.of(Schema.Type.STRING))));
    rowSchema.getField("nonExisting");
  }

  @Test(expected = NotFoundException.class)
  public void testGetNullField() throws Exception {
    DefaultMutableRowSchema rowSchema = new DefaultMutableRowSchema(
      Schema.recordOf("record", Schema.Field.of("strCol", Schema.of(Schema.Type.STRING))));
    rowSchema.getField(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetNullField() {
    DefaultMutableRowSchema rowSchema = new DefaultMutableRowSchema(
      Schema.recordOf("record", Schema.Field.of("strCol", Schema.of(Schema.Type.STRING))));
    rowSchema.setField(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetNullFieldName() {
    DefaultMutableRowSchema rowSchema = new DefaultMutableRowSchema(
      Schema.recordOf("record", Schema.Field.of("strCol", Schema.of(Schema.Type.STRING))));
    rowSchema.setField(Schema.Field.of(null, Schema.of(Schema.Type.STRING)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRenameNullFieldName() {
    DefaultMutableRowSchema rowSchema = new DefaultMutableRowSchema(
      Schema.recordOf("record", Schema.Field.of("strCol", Schema.of(Schema.Type.STRING))));
    rowSchema.renameField(null, "newName");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRenameToNullFieldName() {
    DefaultMutableRowSchema rowSchema = new DefaultMutableRowSchema(
      Schema.recordOf("record", Schema.Field.of("strCol", Schema.of(Schema.Type.STRING))));
    rowSchema.renameField("strCol", null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRenameNonexistingFieldName() {
    DefaultMutableRowSchema rowSchema = new DefaultMutableRowSchema(
      Schema.recordOf("record", Schema.Field.of("strCol", Schema.of(Schema.Type.STRING))));
    rowSchema.renameField("non-existing", "newName");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRenameToExisting() {
    DefaultMutableRowSchema rowSchema = new DefaultMutableRowSchema(
      Schema.recordOf("record",
                      Schema.Field.of("strCol", Schema.of(Schema.Type.STRING)),
                      Schema.Field.of("intCol", Schema.of(Schema.Type.INT))));
    rowSchema.renameField("strCol", "intCol");
  }

  @Test
  public void testValidSchema() throws Exception {
    Schema.Field strField = Schema.Field.of("strCol", Schema.of(Schema.Type.STRING));
    Schema.Field intField = Schema.Field.of("intCol", Schema.of(Schema.Type.INT));
    Schema.Field boolField = Schema.Field.of("intCol", Schema.of(Schema.Type.BOOLEAN));
    Schema.Field newField = Schema.Field.of("newCol", Schema.of(Schema.Type.INT));

    DefaultMutableRowSchema rowSchema = new DefaultMutableRowSchema(
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
    rowSchema.setField(boolField);
    field = rowSchema.getField("intCol");
    Assert.assertEquals(boolField.getName(), field.getName());
    Assert.assertEquals(boolField.getSchema(), field.getSchema());

    //add a field
    rowSchema.setField(newField);
    field = rowSchema.getField("newCol");
    Assert.assertEquals(newField.getName(), field.getName());
    Assert.assertEquals(newField.getSchema(), field.getSchema());


    //rename
    rowSchema.renameField("strCol", "strCol1");
    field = rowSchema.getField("strCol1");
    Assert.assertEquals("strCol1", field.getName());
    Assert.assertEquals(strField.getSchema(), field.getSchema());
    Assert.assertEquals("strCol1", rowSchema.getRenameInfo().getNewName("strCol"));
  }

  @Test
  public void testRenameToSame() throws Exception {
    Schema.Field strField = Schema.Field.of("strCol", Schema.of(Schema.Type.STRING));
    DefaultMutableRowSchema rowSchema = new DefaultMutableRowSchema(Schema.recordOf("record", strField));
    rowSchema.renameField("strCol", "strCol");
    Schema.Field newField = rowSchema.getField("strCol");
    Assert.assertEquals(strField.getName(), newField.getName());
    Assert.assertEquals(strField.getSchema(), newField.getSchema());
    ColumnRenameInfo renameInfo = rowSchema.getRenameInfo();
    Assert.assertEquals("strCol", renameInfo.getNewName("strCol"));
  }

  @Test
  public void testRenameTwice() throws Exception {
    Schema.Field strField = Schema.Field.of("strCol", Schema.of(Schema.Type.STRING));
    DefaultMutableRowSchema rowSchema = new DefaultMutableRowSchema(Schema.recordOf("record", strField));
    rowSchema.renameField("strCol", "newName");
    rowSchema.renameField("newName", "newName1");

    Schema.Field newField = rowSchema.getField("newName1");
    Assert.assertEquals("newName1", newField.getName());
    Assert.assertEquals(strField.getSchema(), newField.getSchema());
    ColumnRenameInfo renameInfo = rowSchema.getRenameInfo();
    Assert.assertEquals("newName1", renameInfo.getNewName("strCol"));
  }

  @Test
  public void testRenameBack() throws Exception {
    Schema.Field strField = Schema.Field.of("strCol", Schema.of(Schema.Type.STRING));
    DefaultMutableRowSchema rowSchema = new DefaultMutableRowSchema(Schema.recordOf("record", strField));
    rowSchema.renameField("strCol", "newName");
    rowSchema.renameField("newName", "strCol");
    Schema.Field newField = rowSchema.getField("strCol");
    Assert.assertEquals(strField.getName(), newField.getName());
    Assert.assertEquals(strField.getSchema(), newField.getSchema());
    ColumnRenameInfo renameInfo = rowSchema.getRenameInfo();
    Assert.assertEquals("strCol", renameInfo.getNewName("strCol"));
  }
}
