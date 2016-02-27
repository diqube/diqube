/**
 * diqube: Distributed Query Base.
 *
 * Copyright (C) 2015 Bastian Gloeckle
 *
 * This file is part of diqube.
 *
 * diqube is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.diqube.itest.tests;

import org.diqube.itest.AbstractDiqubeIntegrationTest;
import org.diqube.itest.annotations.NeedsServer;
import org.diqube.itest.util.Waiter;
import org.diqube.remote.query.thrift.ROptionalTableMetadata;
import org.diqube.server.ControlFileManager;
import org.diqube.thrift.base.thrift.FieldMetadata;
import org.diqube.thrift.base.thrift.FieldType;
import org.diqube.thrift.base.thrift.TableMetadata;
import org.diqube.thrift.base.thrift.Ticket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests whether {@link TableMetadata} publishing works across the cluster.
 *
 * @author Bastian Gloeckle
 */
public class TableMetadataIntegrationTest extends AbstractDiqubeIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(TableMetadataIntegrationTest.class);

  private static final String TABLE = "testtable";
  private static final String FIELD1_CONTROL_FILE_ROW_0 = "/" + TableMetadataIntegrationTest.class.getSimpleName()
      + "/field1_rowId0" + ControlFileManager.CONTROL_FILE_EXTENSION;
  private static final String FIELD1_CONTROL_FILE_ROW_11 = "/" + TableMetadataIntegrationTest.class.getSimpleName()
      + "/field1_rowId11" + ControlFileManager.CONTROL_FILE_EXTENSION;
  private static final String FIELD1_JSON_FILE =
      "/" + TableMetadataIntegrationTest.class.getSimpleName() + "/field1.json";
  private static final String FIELD2_CONTROL_FILE_ROW_11 = "/" + TableMetadataIntegrationTest.class.getSimpleName()
      + "/field2_rowId11" + ControlFileManager.CONTROL_FILE_EXTENSION;
  private static final String FIELD2_JSON_FILE =
      "/" + TableMetadataIntegrationTest.class.getSimpleName() + "/field2.json";

  @Test
  @NeedsServer(servers = 2)
  public void deployOneOnly() {
    serverControl.get(0).deploy(cp(FIELD1_CONTROL_FILE_ROW_0), cp(FIELD1_JSON_FILE));

    Ticket s = serverControl.get(0).loginSuperuser();

    serverControl.get(1).getSerivceTestUtil().tableMetadataService(tableMetadataService -> {
      ROptionalTableMetadata m = tableMetadataService.getTableMetadata(s, TABLE);

      Assert.assertTrue(m.isSetTableMetadata(), "Expected to receive table metadata");
      Assert.assertEquals(m.getTableMetadata().getTableName(), TABLE,
          "Expected correct table name in returned metadata");
      Assert.assertEquals(m.getTableMetadata().getFields().size(), 1, "Expected correct number of fields in metadata");
      FieldMetadata fieldMetadata = m.getTableMetadata().getFields().get(0);
      Assert.assertEquals(fieldMetadata.getFieldName(), "field1", "Expected correct field name");
      Assert.assertEquals(fieldMetadata.getFieldType(), FieldType.LONG, "Expected correct field type");
      Assert.assertEquals(fieldMetadata.isRepeated(), false, "Expected correct repeated state of field");
    });
  }

  @Test
  @NeedsServer(servers = 2)
  public void deployTwoSameOnly() {
    serverControl.get(0).deploy(cp(FIELD1_CONTROL_FILE_ROW_0), cp(FIELD1_JSON_FILE));
    serverControl.get(1).deploy(cp(FIELD1_CONTROL_FILE_ROW_11), cp(FIELD1_JSON_FILE));

    Ticket s = serverControl.get(0).loginSuperuser();

    serverControl.get(1).getSerivceTestUtil().tableMetadataService(tableMetadataService -> {
      ROptionalTableMetadata m = tableMetadataService.getTableMetadata(s, TABLE);

      Assert.assertTrue(m.isSetTableMetadata(), "Expected to receive table metadata");
      Assert.assertEquals(m.getTableMetadata().getTableName(), TABLE,
          "Expected correct table name in returned metadata");
      Assert.assertEquals(m.getTableMetadata().getFields().size(), 1, "Expected correct number of fields in metadata");
      FieldMetadata fieldMetadata = m.getTableMetadata().getFields().get(0);
      Assert.assertEquals(fieldMetadata.getFieldName(), "field1", "Expected correct field name");
      Assert.assertEquals(fieldMetadata.getFieldType(), FieldType.LONG, "Expected correct field type");
      Assert.assertEquals(fieldMetadata.isRepeated(), false, "Expected correct repeated state of field");
    });
  }

  @Test
  @NeedsServer(servers = 2)
  public void deployTwoDifferent() {
    serverControl.get(0).deploy(cp(FIELD1_CONTROL_FILE_ROW_0), cp(FIELD1_JSON_FILE));
    serverControl.get(1).deploy(cp(FIELD2_CONTROL_FILE_ROW_11), cp(FIELD2_JSON_FILE));

    Ticket s = serverControl.get(0).loginSuperuser();

    serverControl.get(0).getSerivceTestUtil().tableMetadataService(tableMetadataService -> {
      ROptionalTableMetadata m = tableMetadataService.getTableMetadata(s, TABLE);

      Assert.assertTrue(m.isSetTableMetadata(), "Expected to receive table metadata");
      Assert.assertEquals(m.getTableMetadata().getTableName(), TABLE,
          "Expected correct table name in returned metadata");
      Assert.assertEquals(m.getTableMetadata().getFields().size(), 2, "Expected correct number of fields in metadata");

      FieldMetadata fieldMetadata = m.getTableMetadata().getFields().get(0);
      if (fieldMetadata.getFieldName().equals("field1")) {
        Assert.assertEquals(fieldMetadata.getFieldType(), FieldType.LONG, "Expected correct field type");
        Assert.assertEquals(fieldMetadata.isRepeated(), false, "Expected correct repeated state of field");
      } else {
        Assert.assertEquals(fieldMetadata.getFieldName(), "field2", "Expected correct field name");
        Assert.assertEquals(fieldMetadata.getFieldType(), FieldType.STRING, "Expected correct field type");
        Assert.assertEquals(fieldMetadata.isRepeated(), false, "Expected correct repeated state of field");
      }

      fieldMetadata = m.getTableMetadata().getFields().get(1);
      if (fieldMetadata.getFieldName().equals("field1")) {
        Assert.assertEquals(fieldMetadata.getFieldType(), FieldType.LONG, "Expected correct field type");
        Assert.assertEquals(fieldMetadata.isRepeated(), false, "Expected correct repeated state of field");
      } else {
        Assert.assertEquals(fieldMetadata.getFieldName(), "field2", "Expected correct field name");
        Assert.assertEquals(fieldMetadata.getFieldType(), FieldType.STRING, "Expected correct field type");
        Assert.assertEquals(fieldMetadata.isRepeated(), false, "Expected correct repeated state of field");
      }
    });
  }

  @Test
  @NeedsServer(servers = 2)
  public void deployTwoDifferentUndeployAgain() {
    serverControl.get(0).deploy(cp(FIELD1_CONTROL_FILE_ROW_0), cp(FIELD1_JSON_FILE));
    serverControl.get(1).deploy(cp(FIELD2_CONTROL_FILE_ROW_11), cp(FIELD2_JSON_FILE));

    Ticket s = serverControl.get(0).loginSuperuser();

    serverControl.get(0).undeploy(cp(FIELD1_CONTROL_FILE_ROW_0));

    serverControl.get(0).getSerivceTestUtil().tableMetadataService(tableMetadataService -> {
      new Waiter().waitUntil("New table metadata is available", 10, 500, () -> {
        ROptionalTableMetadata m;
        try {
          m = tableMetadataService.getTableMetadata(s, TABLE);
        } catch (Exception e) {
          logger.warn("Excpetion when trying to fetch metadata", e);
          return false;
        }
        return m.isSetTableMetadata() && m.getTableMetadata().getFields().size() == 1;
      });

      ROptionalTableMetadata m = tableMetadataService.getTableMetadata(s, TABLE);

      Assert.assertTrue(m.isSetTableMetadata(), "Expected to receive table metadata");
      Assert.assertEquals(m.getTableMetadata().getTableName(), TABLE,
          "Expected correct table name in returned metadata");
      Assert.assertEquals(m.getTableMetadata().getFields().size(), 1, "Expected correct number of fields in metadata");

      FieldMetadata fieldMetadata = m.getTableMetadata().getFields().get(0);
      Assert.assertEquals(fieldMetadata.getFieldName(), "field2", "Expected correct field name");
      Assert.assertEquals(fieldMetadata.getFieldType(), FieldType.STRING, "Expected correct field type");
      Assert.assertEquals(fieldMetadata.isRepeated(), false, "Expected correct repeated state of field");
    });
  }
}
