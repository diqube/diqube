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
import org.diqube.itest.util.QueryResultServiceTestUtil;
import org.diqube.itest.util.QueryResultServiceTestUtil.TestQueryResultService;
import org.diqube.itest.util.ServiceTestUtil;
import org.diqube.itest.util.Waiter;
import org.diqube.remote.base.thrift.RUUID;
import org.diqube.remote.base.util.RUuidUtil;
import org.diqube.server.NewDataWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests execution of specific diql queries.
 *
 * @author Bastian Gloeckle
 */
public class DiqlExecutionIntegrationTest extends AbstractDiqubeIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(DiqlExecutionIntegrationTest.class);

  private static final String AGE_TABLE = "age";
  private static final String AGE_JSON_FILE = "/" + DiqlExecutionIntegrationTest.class.getSimpleName() + "/age.json";
  private static final String AGE_0_CONTROL_FILE =
      "/" + DiqlExecutionIntegrationTest.class.getSimpleName() + "/age_rowId0" + NewDataWatcher.CONTROL_FILE_EXTENSION;
  private static final String AGE_11_CONTROL_FILE =
      "/" + DiqlExecutionIntegrationTest.class.getSimpleName() + "/age_rowId11" + NewDataWatcher.CONTROL_FILE_EXTENSION;

  @Test
  @NeedsServer(servers = 1)
  public void queryProjectedWhereEmptyResult() throws InterruptedException {
    // WHEN
    serverControl.get(0).deploy(cp(AGE_0_CONTROL_FILE), cp(AGE_JSON_FILE));
    serverControl.get(0).deploy(cp(AGE_11_CONTROL_FILE), cp(AGE_JSON_FILE));

    // THEN
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      ServiceTestUtil.queryService(serverControl.get(0),
          (queryService) -> queryService.asyncExecuteQuery(queryUuid,
              "select age, count() from age where add(age, 1) > 100 group by age", true,
              queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received", 10, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);

      Assert.assertFalse(queryRes.getFinalUpdate().isSetRows(),
          "Expected to not get a result, got: " + queryRes.getFinalUpdate());
    } catch (IOException e) {
      throw new RuntimeException("Could not execute query", e);
    }
  }

  @Test
  @NeedsServer(servers = 1)
  public void queryEmptyHavingResult() throws InterruptedException {
    // WHEN
    serverControl.get(0).deploy(cp(AGE_0_CONTROL_FILE), cp(AGE_JSON_FILE));
    serverControl.get(0).deploy(cp(AGE_11_CONTROL_FILE), cp(AGE_JSON_FILE));

    // THEN
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      ServiceTestUtil.queryService(serverControl.get(0),
          (queryService) -> queryService.asyncExecuteQuery(queryUuid,
              "select age, count() from age group by age having count() > 100", true,
              queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received", 10, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);

      Assert.assertFalse(queryRes.getFinalUpdate().isSetRows(),
          "Expected to not get a result, got: " + queryRes.getFinalUpdate());
    } catch (IOException e) {
      throw new RuntimeException("Could not execute query", e);
    }
  }

}
