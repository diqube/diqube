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

import java.io.IOException;
import java.util.UUID;

import org.diqube.itest.AbstractDiqubeIntegrationTest;
import org.diqube.itest.annotations.NeedsServer;
import org.diqube.itest.control.ServerControl;
import org.diqube.itest.util.QueryResultServiceTestUtil;
import org.diqube.itest.util.QueryResultServiceTestUtil.TestQueryResultService;
import org.diqube.itest.util.Waiter;
import org.diqube.name.FlattenedTableNameUtil;
import org.diqube.permission.Permissions;
import org.diqube.remote.query.thrift.ROptionalTableMetadata;
import org.diqube.server.ControlFileManager;
import org.diqube.thrift.base.thrift.AuthorizationException;
import org.diqube.thrift.base.thrift.FieldMetadata;
import org.diqube.thrift.base.thrift.FieldType;
import org.diqube.thrift.base.thrift.RUUID;
import org.diqube.thrift.base.thrift.TableMetadata;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.thrift.base.util.RUuidUtil;
import org.diqube.tool.im.AddPermissionActualIdentityToolFunction;
import org.diqube.tool.im.CreateUserActualIdentityToolFunction;
import org.diqube.util.Holder;
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
  private static final String FLATTEN_CONTROL_FILE = "/" + TableMetadataIntegrationTest.class.getSimpleName()
      + "/flattendata0" + ControlFileManager.CONTROL_FILE_EXTENSION;
  private static final String FLATTEN_AUTOFLATTEN_CONTROL_FILE =
      "/" + TableMetadataIntegrationTest.class.getSimpleName() + "/flattendata0_autoflatten"
          + ControlFileManager.CONTROL_FILE_EXTENSION;
  private static final String FLATTEN_JSON_FILE =
      "/" + TableMetadataIntegrationTest.class.getSimpleName() + "/flattendata.json";

  private static final String USER = "user1";
  private static final String PASSWD = "passwd";
  private static final String EMAIL = "a@b.c";

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

  @Test
  @NeedsServer
  public void flattenByQueryMetadataPublished() {
    serverControl.get(0).deploy(cp(FLATTEN_CONTROL_FILE), cp(FLATTEN_JSON_FILE));

    Ticket s = serverControl.get(0).loginSuperuser();

    logger.info("Starting to execute query whcih should trigger flattening");
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      serverControl.get(0).getSerivceTestUtil()
          .queryService((queryService) -> queryService.asyncExecuteQuery(s, queryUuid,
              "select a.b, count() from flatten(" + TABLE + ", a[*]) group by a.b order by a.b asc", true,
              queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received, flattening therefore done", 10, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);
    } catch (IOException e) {
      throw new RuntimeException("Could not execute query", e);
    }

    logger.info("Accessing ClusterFlattenService to find the UUID of the flattening");
    Holder<RUUID> flatteningRUuid = new Holder<>();
    new Waiter().waitUntil("Newest flattening info is available on server", 2, 200, () -> {
      serverControl.get(0).getSerivceTestUtil().clusterFlattenService(clusterFlattenService -> {
        flatteningRUuid.setValue(clusterFlattenService.getLatestValidFlattening(TABLE, "a[*]").getUuid());
      });
      return flatteningRUuid.getValue() != null;
    });

    FlattenedTableNameUtil flattenedTableNameGenerator = new FlattenedTableNameUtil();
    String flattenedTableName = flattenedTableNameGenerator.createFlattenedTableName(TABLE, "a[*]",
        RUuidUtil.toUuid(flatteningRUuid.getValue()));

    logger.info("Checking if metadata is available for flattened table.");
    serverControl.get(0).getSerivceTestUtil().tableMetadataService(tableMetadataService -> {
      new Waiter().waitUntil("Metadata of flattened table is available", 10, 500, () -> {
        ROptionalTableMetadata m;
        try {
          m = tableMetadataService.getTableMetadata(s, flattenedTableName);
        } catch (Exception e) {
          logger.warn("Excpetion when trying to fetch metadata", e);
          return false;
        }
        return m.isSetTableMetadata() && m.getTableMetadata().getFields().size() == 2;
      });

      ROptionalTableMetadata m = tableMetadataService.getTableMetadata(s, flattenedTableName);

      Assert.assertTrue(m.isSetTableMetadata(), "Expected to receive table metadata");
      Assert.assertEquals(m.getTableMetadata().getTableName(), flattenedTableName,
          "Expected correct table name in returned metadata");
      Assert.assertEquals(m.getTableMetadata().getFields().size(), 2, "Expected correct number of fields in metadata");

      FieldMetadata fieldMetadata = m.getTableMetadata().getFields().get(0);
      if (fieldMetadata.getFieldName().equals("a")) {
        Assert.assertEquals(fieldMetadata.getFieldType(), FieldType.CONTAINER, "Expected correct field type");
        Assert.assertEquals(fieldMetadata.isRepeated(), false, "Expected correct repeated state of field");
      } else {
        Assert.assertEquals(fieldMetadata.getFieldName(), "a.b", "Expected correct field name");
        Assert.assertEquals(fieldMetadata.getFieldType(), FieldType.LONG, "Expected correct field type");
        Assert.assertEquals(fieldMetadata.isRepeated(), false, "Expected correct repeated state of field");
      }
    });
  }

  @Test
  @NeedsServer
  public void flattenByAutoflatten() {
    serverControl.get(0).deploy(cp(FLATTEN_AUTOFLATTEN_CONTROL_FILE), cp(FLATTEN_JSON_FILE));

    Ticket s = serverControl.get(0).loginSuperuser();

    logger.info("Accessing ClusterFlattenService to find the UUID of the flattening");
    Holder<RUUID> flatteningRUuid = new Holder<>();
    new Waiter().waitUntil("Newest flattening info is available on server", 2, 200, () -> {
      serverControl.get(0).getSerivceTestUtil().clusterFlattenService(clusterFlattenService -> {
        flatteningRUuid.setValue(clusterFlattenService.getLatestValidFlattening(TABLE, "a[*]").getUuid());
      });
      return flatteningRUuid.getValue() != null;
    });

    FlattenedTableNameUtil flattenedTableNameGenerator = new FlattenedTableNameUtil();
    String flattenedTableName = flattenedTableNameGenerator.createFlattenedTableName(TABLE, "a[*]",
        RUuidUtil.toUuid(flatteningRUuid.getValue()));

    logger.info("Checking if metadata is available for flattened table.");
    serverControl.get(0).getSerivceTestUtil().tableMetadataService(tableMetadataService -> {
      new Waiter().waitUntil("Metadata of flattened table is available", 10, 500, () -> {
        ROptionalTableMetadata m;
        try {
          m = tableMetadataService.getTableMetadata(s, flattenedTableName);
        } catch (Exception e) {
          logger.warn("Excpetion when trying to fetch metadata", e);
          return false;
        }
        return m.isSetTableMetadata() && m.getTableMetadata().getFields().size() == 2;
      });

      ROptionalTableMetadata m = tableMetadataService.getTableMetadata(s, flattenedTableName);

      Assert.assertTrue(m.isSetTableMetadata(), "Expected to receive table metadata");
      Assert.assertEquals(m.getTableMetadata().getTableName(), flattenedTableName,
          "Expected correct table name in returned metadata");
      Assert.assertEquals(m.getTableMetadata().getFields().size(), 2, "Expected correct number of fields in metadata");

      FieldMetadata fieldMetadata = m.getTableMetadata().getFields().get(0);
      if (fieldMetadata.getFieldName().equals("a")) {
        Assert.assertEquals(fieldMetadata.getFieldType(), FieldType.CONTAINER, "Expected correct field type");
        Assert.assertEquals(fieldMetadata.isRepeated(), false, "Expected correct repeated state of field");
      } else {
        Assert.assertEquals(fieldMetadata.getFieldName(), "a.b", "Expected correct field name");
        Assert.assertEquals(fieldMetadata.getFieldType(), FieldType.LONG, "Expected correct field type");
        Assert.assertEquals(fieldMetadata.isRepeated(), false, "Expected correct repeated state of field");
      }
    });
  }

  @Test
  @NeedsServer
  public void flattenByAutoflattenAndIncompleteFlattenTableNameQuery() {
    serverControl.get(0).deploy(cp(FLATTEN_AUTOFLATTEN_CONTROL_FILE), cp(FLATTEN_JSON_FILE));

    Ticket s = serverControl.get(0).loginSuperuser();

    // this one does not include the flattenId! It is more like the statement used in a diql query.
    String incompleteFlattenTableName = "flatten(" + TABLE + ", a[*])";

    logger.info("Checking if metadata is available for flattened table.");
    serverControl.get(0).getSerivceTestUtil().tableMetadataService(tableMetadataService -> {
      new Waiter().waitUntil("Metadata of flattened table is available", 10, 500, () -> {
        ROptionalTableMetadata m;
        try {
          m = tableMetadataService.getTableMetadata(s, incompleteFlattenTableName);
        } catch (Exception e) {
          logger.warn("Excpetion when trying to fetch metadata", e);
          return false;
        }
        return m.isSetTableMetadata() && m.getTableMetadata().getFields().size() == 2;
      });

      ROptionalTableMetadata m = tableMetadataService.getTableMetadata(s, incompleteFlattenTableName);

      Assert.assertTrue(m.isSetTableMetadata(), "Expected to receive table metadata");
      // the function should NOT return the table name with the flattenId. If the user did not know the flattenId, we do
      // not want to tell him - because in the end this is some internal information he should not care about!
      Assert.assertEquals(m.getTableMetadata().getTableName(), incompleteFlattenTableName,
          "Expected correct table name in returned metadata");
      Assert.assertEquals(m.getTableMetadata().getFields().size(), 2, "Expected correct number of fields in metadata");

      FieldMetadata fieldMetadata = m.getTableMetadata().getFields().get(0);
      if (fieldMetadata.getFieldName().equals("a")) {
        Assert.assertEquals(fieldMetadata.getFieldType(), FieldType.CONTAINER, "Expected correct field type");
        Assert.assertEquals(fieldMetadata.isRepeated(), false, "Expected correct repeated state of field");
      } else {
        Assert.assertEquals(fieldMetadata.getFieldName(), "a.b", "Expected correct field name");
        Assert.assertEquals(fieldMetadata.getFieldType(), FieldType.LONG, "Expected correct field type");
        Assert.assertEquals(fieldMetadata.isRepeated(), false, "Expected correct repeated state of field");
      }
    });
  }

  @Test
  @NeedsServer
  public void flattenByAutoflattenNonRoot() {
    serverControl.get(0).deploy(cp(FLATTEN_AUTOFLATTEN_CONTROL_FILE), cp(FLATTEN_JSON_FILE));

    logger.info("Creating new user with access to test table");
    toolControl.im(serverControl.get(0).getAddr(), CreateUserActualIdentityToolFunction.FUNCTION_NAME, //
        ServerControl.ROOT_USER, ServerControl.ROOT_PASSWORD, //
        USER, // paramUser
        PASSWD, // paramPassword
        EMAIL, // paramEmail
        null, // paramPermission
        null // paramPermissionObject
    );

    toolControl.im(serverControl.get(0).getAddr(), AddPermissionActualIdentityToolFunction.FUNCTION_NAME, //
        ServerControl.ROOT_USER, ServerControl.ROOT_PASSWORD, //
        USER, // paramUser
        null, // paramPassword
        null, // paramEmail
        Permissions.TABLE_ACCESS, // paramPermission
        TABLE // paramPermissionObject
    );

    Ticket ticket = serverControl.get(0).login(USER, PASSWD);

    // this one does not include the flattenId! It is more like the statement used in a diql query.
    String incompleteFlattenTableName = "flatten(" + TABLE + ", a[*])";

    logger.info("Checking if metadata is available for flattened table.");
    serverControl.get(0).getSerivceTestUtil().tableMetadataService(tableMetadataService -> {
      new Waiter().waitUntil("Metadata of flattened table is available", 10, 500, () -> {
        ROptionalTableMetadata m;
        try {
          m = tableMetadataService.getTableMetadata(ticket, incompleteFlattenTableName);
        } catch (Exception e) {
          logger.warn("Excpetion when trying to fetch metadata", e);
          return false;
        }
        return m.isSetTableMetadata() && m.getTableMetadata().getFields().size() == 2;
      });

      ROptionalTableMetadata m = tableMetadataService.getTableMetadata(ticket, incompleteFlattenTableName);

      Assert.assertTrue(m.isSetTableMetadata(), "Expected to receive table metadata");
      // the function should NOT return the table name with the flattenId. If the user did not know the flattenId, we do
      // not want to tell him - because in the end this is some internal information he should not care about!
      Assert.assertEquals(m.getTableMetadata().getTableName(), incompleteFlattenTableName,
          "Expected correct table name in returned metadata");
      Assert.assertEquals(m.getTableMetadata().getFields().size(), 2, "Expected correct number of fields in metadata");

      FieldMetadata fieldMetadata = m.getTableMetadata().getFields().get(0);
      if (fieldMetadata.getFieldName().equals("a")) {
        Assert.assertEquals(fieldMetadata.getFieldType(), FieldType.CONTAINER, "Expected correct field type");
        Assert.assertEquals(fieldMetadata.isRepeated(), false, "Expected correct repeated state of field");
      } else {
        Assert.assertEquals(fieldMetadata.getFieldName(), "a.b", "Expected correct field name");
        Assert.assertEquals(fieldMetadata.getFieldType(), FieldType.LONG, "Expected correct field type");
        Assert.assertEquals(fieldMetadata.isRepeated(), false, "Expected correct repeated state of field");
      }
    });
  }

  @Test
  @NeedsServer
  public void flattenByAutoflattenNonRootNoAccess() {
    serverControl.get(0).deploy(cp(FLATTEN_AUTOFLATTEN_CONTROL_FILE), cp(FLATTEN_JSON_FILE));

    logger.info("Creating new user WITHOUT access to test table");
    toolControl.im(serverControl.get(0).getAddr(), CreateUserActualIdentityToolFunction.FUNCTION_NAME, //
        ServerControl.ROOT_USER, ServerControl.ROOT_PASSWORD, //
        USER, // paramUser
        PASSWD, // paramPassword
        EMAIL, // paramEmail
        null, // paramPermission
        null // paramPermissionObject
    );

    Ticket ticket = serverControl.get(0).login(USER, PASSWD);
    Ticket superuserTicket = serverControl.get(0).loginSuperuser();

    // this one does not include the flattenId! It is more like the statement used in a diql query.
    String incompleteFlattenTableName = "flatten(" + TABLE + ", a[*])";

    logger.info("Checking if metadata is available for flattened table.");
    serverControl.get(0).getSerivceTestUtil().tableMetadataService(tableMetadataService -> {
      new Waiter().waitUntil("Metadata of flattened table is available for superuser", 10, 500, () -> {
        ROptionalTableMetadata m;
        try {
          m = tableMetadataService.getTableMetadata(superuserTicket, incompleteFlattenTableName);
        } catch (Exception e) {
          logger.warn("Excpetion when trying to fetch metadata", e);
          return false;
        }
        return m.isSetTableMetadata() && m.getTableMetadata().getFields().size() == 2;
      });

      try {
        ROptionalTableMetadata res = tableMetadataService.getTableMetadata(ticket, incompleteFlattenTableName);
        Assert.fail("Did expect that service returns with "
            + "authorizationException, but did return something instead: " + res);
      } catch (AuthorizationException e) {
        // fine, exception is expected
      }
    });
  }
}
