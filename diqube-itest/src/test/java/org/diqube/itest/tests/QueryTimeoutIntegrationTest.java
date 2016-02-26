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

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;

import org.diqube.config.ConfigKey;
import org.diqube.itest.AbstractDiqubeIntegrationTest;
import org.diqube.itest.annotations.NeedsProcessPid;
import org.diqube.itest.annotations.NeedsServer;
import org.diqube.itest.control.ServerControl;
import org.diqube.itest.util.QueryResultServiceTestUtil;
import org.diqube.itest.util.QueryResultServiceTestUtil.TestQueryResultService;
import org.diqube.itest.util.TestDataGenerator;
import org.diqube.itest.util.Waiter;
import org.diqube.server.ControlFileManager;
import org.diqube.thrift.base.thrift.RUUID;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.thrift.base.util.RUuidUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

/**
 * Tests that queries timeout.
 *
 * @author Bastian Gloeckle
 */
public class QueryTimeoutIntegrationTest extends AbstractDiqubeIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(QueryTimeoutIntegrationTest.class);

  private static final String BIG_TABLE = "big";
  private static final String BIG_CONTROL_FILE =
      "/" + QueryTimeoutIntegrationTest.class.getSimpleName() + "/big" + ControlFileManager.CONTROL_FILE_EXTENSION;
  private static final String BIG_DATA_FILE_WORK = "big.json";

  @Test
  @NeedsServer(servers = 1, manualStart = true)
  @NeedsProcessPid
  public void queryProjectedWhereEmptyResult() throws InterruptedException, FileNotFoundException, IOException {
    // GIVEN
    // a table which has enough data that the first queries on it might time out
    TestDataGenerator.generateJsonTestData(work(BIG_DATA_FILE_WORK), 10, 2, new String[] { "a", "b" }, 30);

    // WHEN
    // start server with a very low timeout
    serverControl.get(0).start(prop -> prop.setProperty(ConfigKey.QUERY_EXECUTION_TIMEOUT_SECONDS, "1"));
    serverControl.get(0).deploy(cp(BIG_CONTROL_FILE), work(BIG_DATA_FILE_WORK));

    Ticket ticket = serverControl.get(0).loginSuperuser();

    // THEN
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      UUID queryUuid = UUID.randomUUID();
      RUUID queryRUuid = RUuidUtil.toRUuid(queryUuid);
      logger.info("Executing query {}", RUuidUtil.toUuid(queryRUuid));
      // execute a long-running query. It should just take longer than the timeout we set above...
      serverControl.get(0).getSerivceTestUtil()
          .queryService((queryService) -> queryService.asyncExecuteQuery(ticket, queryRUuid,
              "select avg(add(a[*].a[*], 1)), avg(add(a[*].b[*], 1)), avg(add(b[*].a[*], 1)) from " + BIG_TABLE, true,
              queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Expected to receive a timeout exception.", 10, 500,
          () -> queryRes.getException() != null);

      // we received an exception, validate that all threads of the query have been shut down!

      // Actually, we should not wait too long here, otherwise the worker threads could end because they finished their
      // work (but we want them to be terminated here!), but in this test case, the worker threads will be busy building
      // columns (with the results of the projections/colAggregations and that process does not have enough Object#wait
      // calls inside to recognoze the interruption early. So: Stick with 10s although its not perfect :/
      new Waiter().waitUntil("Expected that all threads of the query were shut down", 10, 1000,
          () -> !threadDumpContainsQueryUuid(serverControl.get(0), queryUuid));

      logger.info("Successfully received exception and all threads of the query were shut down.",
          queryRes.getException());
    } catch (IOException e) {
      throw new RuntimeException("Could not execute query", e);
    }
  }

  private boolean threadDumpContainsQueryUuid(ServerControl serverControl, UUID queryUuid) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    serverControl.createThreadDump(baos);
    String s = baos.toString();

    logger.info("Gathered thread dump of server process: {}", s);

    return s.contains(queryUuid.toString());
  }

}
