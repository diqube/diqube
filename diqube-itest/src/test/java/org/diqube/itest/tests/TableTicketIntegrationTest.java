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
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.thrift.TException;
import org.diqube.itest.AbstractDiqubeIntegrationTest;
import org.diqube.itest.annotations.NeedsServer;
import org.diqube.itest.control.ServerControl;
import org.diqube.itest.util.QueryResultServiceTestUtil;
import org.diqube.itest.util.QueryResultServiceTestUtil.TestQueryResultService;
import org.diqube.itest.util.Waiter;
import org.diqube.permission.Permissions;
import org.diqube.server.NewDataWatcher;
import org.diqube.thrift.base.thrift.AuthenticationException;
import org.diqube.thrift.base.thrift.AuthorizationException;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.thrift.base.util.RUuidUtil;
import org.diqube.tool.im.AddPermissionActualIdentityToolFunction;
import org.diqube.tool.im.ChangePasswordActualIdentityToolFunction;
import org.diqube.tool.im.CreateUserActualIdentityToolFunction;
import org.diqube.tool.im.DeletePermissionActualIdentityToolFunction;
import org.diqube.tool.im.DeleteUserActualIdentityToolFunction;
import org.diqube.tool.im.IdentityToolFunction;
import org.diqube.util.Holder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests that services can only be accessed by users that have corresponding permissions.
 * 
 * <p>
 * Uses diqube-tools {@link IdentityToolFunction} to adjust permissions and also tests the vital functions of that.
 *
 * @author Bastian Gloeckle
 */
public class TableTicketIntegrationTest extends AbstractDiqubeIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(TableTicketIntegrationTest.class);

  private static final String FIRST_TABLE = "age";
  private static final String FIRST_CONTROL_FILE =
      "/" + TableTicketIntegrationTest.class.getSimpleName() + "/age" + NewDataWatcher.CONTROL_FILE_EXTENSION;
  private static final String SECOND_TABLE = "age2";
  private static final String SECOND_CONTROL_FILE =
      "/" + TableTicketIntegrationTest.class.getSimpleName() + "/age2" + NewDataWatcher.CONTROL_FILE_EXTENSION;
  private static final String JSON_FILE = "/" + MergeDeployIntegrationTest.class.getSimpleName() + "/age.json";

  private static final String USER = "testUser";
  private static final String PWD = "testPassword";
  private static final String SECOND_PWD = "testPassword2";
  private static final String EMAIL = "a@b.c";

  @Test
  @NeedsServer(servers = 1)
  public void queryUserAllowed() throws IOException {
    // GIVEN
    Ticket t = deployCreateUserWithPermissionsAndLogin();

    // execute queries
    try (TestQueryResultService queryResults = QueryResultServiceTestUtil.createQueryResultService()) {
      UUID queryUuid = UUID.randomUUID();
      logger.info("Executing query {}", queryUuid);
      serverControl.get(0).getSerivceTestUtil()
          .queryService(queryService -> queryService.asyncExecuteQuery(t, RUuidUtil.toRUuid(queryUuid),
              "select age from " + FIRST_TABLE, true, queryResults.getThisServicesAddr().toRNodeAddress()));

      // we have access, so this should succeed!
      new Waiter().waitUntil("Final result of query received", 10, 500,
          () -> queryResults.check() && queryResults.getFinalUpdate() != null);

      // we do NOT have access, so exception is expected.
      try {
        serverControl.get(0).getSerivceTestUtil()
            .queryServiceThrowException(queryService -> queryService.asyncExecuteQuery(t, RUuidUtil.toRUuid(queryUuid),
                "select age from " + SECOND_TABLE, true, queryResults.getThisServicesAddr().toRNodeAddress()));
        Assert.fail("Expected to receive an exception since we do not have permission to access the table!");
      } catch (TException e) {
        // swallow, as this is expected!
        logger.info("Received exception {}", e);
        Assert.assertTrue(e instanceof AuthorizationException,
            "Received exception should be an AuthorizationException");
      }

    }
  }

  @Test
  @NeedsServer(servers = 1)
  public void cancelQueryUserAllowed() throws IOException {
    // GIVEN
    Ticket t = deployCreateUserWithPermissionsAndLogin();
    Ticket rootTicket = serverControl.get(0).loginSuperuser();

    // execute queries
    try (TestQueryResultService queryResults = QueryResultServiceTestUtil.createQueryResultService()) {
      UUID queryUuid = UUID.randomUUID();
      logger.info("Executing query {}", queryUuid);
      serverControl.get(0).getSerivceTestUtil()
          .queryService(queryService -> queryService.asyncExecuteQuery(rootTicket, RUuidUtil.toRUuid(queryUuid),
              "select age from " + FIRST_TABLE, true, queryResults.getThisServicesAddr().toRNodeAddress()));

      // we do NOT have access with non-root-ticket
      try {
        serverControl.get(0).getSerivceTestUtil().queryServiceThrowException(
            queryService -> queryService.cancelQueryExecution(t, RUuidUtil.toRUuid(queryUuid)));
        Assert.fail("Expected to receive an exception since we do not have permission to cancel the query!");
      } catch (TException e) {
        // swallow, as this is expected!
        logger.info("Received exception {}", e);
        Assert.assertTrue(e instanceof AuthorizationException,
            "Received exception should be an AuthorizationException");
      }
    }
  }

  @Test
  @NeedsServer(servers = 1)
  public void tableListOnlyContainsTablesWithPermission() throws IOException {
    // GIVEN
    Ticket t = deployCreateUserWithPermissionsAndLogin();

    Holder<List<String>> h = new Holder<>();
    serverControl.get(0).getSerivceTestUtil()
        .clusterInfoService(infoService -> h.setValue(infoService.getAvailableTables(t)));

    new Waiter().waitUntil("Received list of tables", 5, 100, () -> h.getValue() != null);

    Assert.assertEquals(h.getValue(), Arrays.asList(FIRST_TABLE),
        "Expected to receive correct (permission-filtered) list of tables");
  }

  @Test
  @NeedsServer(servers = 1)
  public void flattenOnlyAllowed() throws IOException {
    // GIVEN
    Ticket t = deployCreateUserWithPermissionsAndLogin();

    try {
      serverControl.get(0).getSerivceTestUtil().flattenPreparationServiceThrowException(
          flattenPrepServ -> flattenPrepServ.prepareForQueriesOnFlattenedTable(t, SECOND_TABLE, "a"));
      Assert.fail("Expected to get exception when trying to flatten table that we do not have access to.");
    } catch (TException e) {
      logger.info("Received exception {}", e);
      Assert.assertTrue(e instanceof AuthorizationException, "Received exception should be an AuthorizationException");
    }

    try {
      // try to flatten on table we have access to. We can't actually flatten the table since the field is not repeated,
      // but anyway, we should not receive an exception (because the server identifies asynchronously that it cannot
      // flatten by that field)
      serverControl.get(0).getSerivceTestUtil().flattenPreparationServiceThrowException(
          flattenPrepServ -> flattenPrepServ.prepareForQueriesOnFlattenedTable(t, FIRST_TABLE, "a"));
    } catch (TException e) {
      logger.info("Received exception {}", e);
      Assert.fail("Received exception although not expected");
    }
  }

  @Test
  @NeedsServer(servers = 1)
  public void permissionRemovedAgain() throws IOException {
    // GIVEN
    Ticket t = deployCreateUserWithPermissionsAndLogin();

    // remove permission from user
    toolControl.im(serverControl.get(0).getAddr(), DeletePermissionActualIdentityToolFunction.FUNCTION_NAME, //
        ServerControl.ROOT_USER, ServerControl.ROOT_PASSWORD, //
        USER, // paramUser
        null, // paramPassword
        null, // paramEmail
        Permissions.TABLE_ACCESS, // paramPermission
        FIRST_TABLE // paramPermissionObject
    );

    Holder<List<String>> h = new Holder<>();
    serverControl.get(0).getSerivceTestUtil()
        .clusterInfoService(infoService -> h.setValue(infoService.getAvailableTables(t)));

    new Waiter().waitUntil("Received list of tables", 5, 100, () -> h.getValue() != null);

    Assert.assertEquals(h.getValue(), Arrays.asList(), // empty list!
        "Expected to receive correct (permission-filtered) list of tables");
  }

  @Test
  @NeedsServer(servers = 1)
  public void userChangePasswordSucceeds() throws IOException {
    // GIVEN
    deployCreateUserWithPermissionsAndLogin();

    // remove permission from user
    toolControl.im(serverControl.get(0).getAddr(), ChangePasswordActualIdentityToolFunction.FUNCTION_NAME, //
        ServerControl.ROOT_USER, ServerControl.ROOT_PASSWORD, //
        USER, // paramUser
        SECOND_PWD, // paramPassword
        null, // paramEmail
        null, // paramPermission
        null // paramPermissionObject
    );

    Ticket t = serverControl.get(0).login(USER, SECOND_PWD);

    Holder<List<String>> h = new Holder<>();
    serverControl.get(0).getSerivceTestUtil()
        .clusterInfoService(infoService -> h.setValue(infoService.getAvailableTables(t)));

    new Waiter().waitUntil("Received list of tables", 5, 100, () -> h.getValue() != null);

    Assert.assertEquals(h.getValue(), Arrays.asList(FIRST_TABLE), // we did not change permissions.
        "Expected to receive correct (permission-filtered) list of tables");
  }

  @Test
  @NeedsServer(servers = 1)
  public void deleteUser() throws IOException {
    // GIVEN
    deployCreateUserWithPermissionsAndLogin();

    // remove permission from user
    toolControl.im(serverControl.get(0).getAddr(), DeleteUserActualIdentityToolFunction.FUNCTION_NAME, //
        ServerControl.ROOT_USER, ServerControl.ROOT_PASSWORD, //
        USER, // paramUser
        null, // paramPassword
        null, // paramEmail
        null, // paramPermission
        null // paramPermissionObject
    );

    Holder<AuthenticationException> authException = new Holder<>();
    serverControl.get(0).getSerivceTestUtil().identityService(identityService -> {
      try {
        identityService.login(USER, PWD);
      } catch (AuthenticationException e) {
        authException.setValue(e);
      }
    });

    new Waiter().waitUntil("AuthenticationException available", 3, 100, () -> authException.getValue() != null);
  }

  private Ticket deployCreateUserWithPermissionsAndLogin() {
    serverControl.get(0).deploy(cp(FIRST_CONTROL_FILE), cp(JSON_FILE));
    serverControl.get(0).deploy(cp(SECOND_CONTROL_FILE), cp(JSON_FILE));

    // new user
    toolControl.im(serverControl.get(0).getAddr(), CreateUserActualIdentityToolFunction.FUNCTION_NAME, //
        ServerControl.ROOT_USER, ServerControl.ROOT_PASSWORD, //
        USER, // paramUser
        PWD, // paramPassword
        EMAIL, // paramEmail
        null, // paramPermission
        null // paramPermissionObject
    );

    // add permission to user
    toolControl.im(serverControl.get(0).getAddr(), AddPermissionActualIdentityToolFunction.FUNCTION_NAME, //
        ServerControl.ROOT_USER, ServerControl.ROOT_PASSWORD, //
        USER, // paramUser
        null, // paramPassword
        null, // paramEmail
        Permissions.TABLE_ACCESS, // paramPermission
        FIRST_TABLE // paramPermissionObject
    );

    // login
    Ticket t = serverControl.get(0).login(USER, PWD);
    return t;
  }

}
