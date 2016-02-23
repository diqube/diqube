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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.diqube.itest.AbstractDiqubeIntegrationTest;
import org.diqube.itest.annotations.NeedsServer;
import org.diqube.itest.util.QueryResultServiceTestUtil;
import org.diqube.itest.util.QueryResultServiceTestUtil.TestQueryResultService;
import org.diqube.itest.util.Waiter;
import org.diqube.itest.util.Waiter.WaitTimeoutException;
import org.diqube.server.ControlFileManager;
import org.diqube.thrift.base.thrift.RUUID;
import org.diqube.thrift.base.thrift.RValue;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.thrift.base.util.RUuidUtil;
import org.diqube.thrift.base.util.RValueUtil;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests simple deployments of data to a cluster.
 *
 * @author Bastian Gloeckle
 */
public class SimpleServerDeployIntegrationTest extends AbstractDiqubeIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(SimpleServerDeployIntegrationTest.class);

  private static final String AGE_TABLE = "age";
  private static final String AGE_JSON_FILE =
      "/" + SimpleServerDeployIntegrationTest.class.getSimpleName() + "/age.json";
  private static final String AGE_0_CONTROL_FILE = "/" + SimpleServerDeployIntegrationTest.class.getSimpleName()
      + "/age_rowId0" + ControlFileManager.CONTROL_FILE_EXTENSION;
  private static final String AGE_11_CONTROL_FILE = "/" + SimpleServerDeployIntegrationTest.class.getSimpleName()
      + "/age_rowId11" + ControlFileManager.CONTROL_FILE_EXTENSION;
  private static final String AGE_10_CONTROL_FILE = "/" + SimpleServerDeployIntegrationTest.class.getSimpleName()
      + "/age_rowId10" + ControlFileManager.CONTROL_FILE_EXTENSION;

  @Test
  @NeedsServer(servers = 1)
  public void singleServerQuery() throws InterruptedException {
    // WHEN
    serverControl.get(0).deploy(cp(AGE_0_CONTROL_FILE), cp(AGE_JSON_FILE));

    Ticket ticket = serverControl.get(0).loginSuperuser();

    // THEN
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      serverControl.get(0).getSerivceTestUtil()
          .queryService((queryService) -> queryService.asyncExecuteQuery(ticket, queryUuid,
              "select age, count() from age group by age order by count() desc", true,
              queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received", 10, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);

      List<Pair<Long, Long>> expected = new ArrayList<>();
      expected.add(new Pair<>(5L, 5L));
      expected.add(new Pair<>(3L, 3L));
      expected.add(new Pair<>(2L, 2L));
      expected.add(new Pair<>(1L, 1L));

      List<Pair<Long, Long>> actual = new ArrayList<>();
      for (List<RValue> row : queryRes.getFinalUpdate().getRows()) {
        Assert.assertEquals(row.size(), 2, "Expected to get correct number of cols returned.");
        actual.add(new Pair<>((Long) RValueUtil.createValue(row.get(0)), (Long) RValueUtil.createValue(row.get(1))));
      }

      Assert.assertEquals(actual, expected, "Expected to get correct results.");
    } catch (IOException e) {
      throw new RuntimeException("Could not execute query", e);
    }
  }

  @Test
  @NeedsServer(servers = 2)
  public void twoServerQuery() throws InterruptedException {
    // WHEN
    serverControl.get(0).deploy(cp(AGE_0_CONTROL_FILE), cp(AGE_JSON_FILE));
    serverControl.get(1).deploy(cp(AGE_11_CONTROL_FILE), cp(AGE_JSON_FILE));

    Ticket ticket = serverControl.get(0).loginSuperuser();

    // THEN
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      serverControl.get(0).getSerivceTestUtil()
          .queryService((queryService) -> queryService.asyncExecuteQuery(ticket, queryUuid,
              "select age, count() from age group by age order by count() desc", true,
              queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received", 10, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);

      List<Pair<Long, Long>> expected = new ArrayList<>();
      expected.add(new Pair<>(5L, 10L));
      expected.add(new Pair<>(3L, 6L));
      expected.add(new Pair<>(2L, 4L));
      expected.add(new Pair<>(1L, 2L));

      List<Pair<Long, Long>> actual = new ArrayList<>();
      for (List<RValue> row : queryRes.getFinalUpdate().getRows()) {
        Assert.assertEquals(row.size(), 2, "Expected to get correct number of cols returned.");
        actual.add(new Pair<>((Long) RValueUtil.createValue(row.get(0)), (Long) RValueUtil.createValue(row.get(1))));
      }

      Assert.assertEquals(actual, expected, "Expected to get correct results.");
    } catch (IOException e) {
      throw new RuntimeException("Could not execute query", e);
    }
  }

  @Test
  @NeedsServer(servers = 1, manualStart = true)
  public void deployThenStart() throws InterruptedException {
    // WHEN
    serverControl.get(0).deploy(cp(AGE_0_CONTROL_FILE), cp(AGE_JSON_FILE));
    serverControl.get(0).start();
    serverControl.get(0).waitUntilDeployed(cp(AGE_0_CONTROL_FILE));

    Ticket ticket = serverControl.get(0).loginSuperuser();

    // THEN
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      serverControl.get(0).getSerivceTestUtil()
          .queryService((queryService) -> queryService.asyncExecuteQuery(ticket, queryUuid,
              "select age, count() from age group by age order by count() desc", true,
              queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received", 10, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);

      List<Pair<Long, Long>> expected = new ArrayList<>();
      expected.add(new Pair<>(5L, 5L));
      expected.add(new Pair<>(3L, 3L));
      expected.add(new Pair<>(2L, 2L));
      expected.add(new Pair<>(1L, 1L));

      List<Pair<Long, Long>> actual = new ArrayList<>();
      for (List<RValue> row : queryRes.getFinalUpdate().getRows()) {
        Assert.assertEquals(row.size(), 2, "Expected to get correct number of cols returned.");
        actual.add(new Pair<>((Long) RValueUtil.createValue(row.get(0)), (Long) RValueUtil.createValue(row.get(1))));
      }

      Assert.assertEquals(actual, expected, "Expected to get correct results.");
    } catch (IOException e) {
      throw new RuntimeException("Could not execute query", e);
    }
  }

  @Test
  @NeedsServer(servers = 2)
  public void undeployInCluster() throws InterruptedException {
    // WHEN
    serverControl.get(0).deploy(cp(AGE_0_CONTROL_FILE), cp(AGE_JSON_FILE));
    serverControl.get(1).deploy(cp(AGE_11_CONTROL_FILE), cp(AGE_JSON_FILE));

    Ticket ticket = serverControl.get(0).loginSuperuser();

    // THEN results from both cluster nodes
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      serverControl.get(0).getSerivceTestUtil()
          .queryService((queryService) -> queryService.asyncExecuteQuery(ticket, queryUuid,
              "select age, count() from age group by age order by count() desc", true,
              queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received", 10, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);

      List<Pair<Long, Long>> expected = new ArrayList<>();
      expected.add(new Pair<>(5L, 10L));
      expected.add(new Pair<>(3L, 6L));
      expected.add(new Pair<>(2L, 4L));
      expected.add(new Pair<>(1L, 2L));

      List<Pair<Long, Long>> actual = new ArrayList<>();
      for (List<RValue> row : queryRes.getFinalUpdate().getRows()) {
        Assert.assertEquals(row.size(), 2, "Expected to get correct number of cols returned.");
        actual.add(new Pair<>((Long) RValueUtil.createValue(row.get(0)), (Long) RValueUtil.createValue(row.get(1))));
      }

      Assert.assertEquals(actual, expected, "Expected to get correct results.");
    } catch (IOException e) {
      throw new RuntimeException("Could not execute query", e);
    }

    // WHEN
    serverControl.get(1).undeploy(cp(AGE_11_CONTROL_FILE));

    // THEN result from one cluster node only
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      serverControl.get(0).getSerivceTestUtil()
          .queryService((queryService) -> queryService.asyncExecuteQuery(ticket, queryUuid,
              "select age, count() from age group by age order by count() desc", true,
              queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received", 10, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);

      List<Pair<Long, Long>> expected = new ArrayList<>();
      expected.add(new Pair<>(5L, 5L));
      expected.add(new Pair<>(3L, 3L));
      expected.add(new Pair<>(2L, 2L));
      expected.add(new Pair<>(1L, 1L));

      List<Pair<Long, Long>> actual = new ArrayList<>();
      for (List<RValue> row : queryRes.getFinalUpdate().getRows()) {
        Assert.assertEquals(row.size(), 2, "Expected to get correct number of cols returned.");
        actual.add(new Pair<>((Long) RValueUtil.createValue(row.get(0)), (Long) RValueUtil.createValue(row.get(1))));
      }

      Assert.assertEquals(actual, expected, "Expected to get correct results.");
    } catch (IOException e) {
      throw new RuntimeException("Could not execute query", e);
    }
  }

  @Test
  @NeedsServer(servers = 1)
  public void singleServerMultipleShards() throws InterruptedException {
    // WHEN
    serverControl.get(0).deploy(cp(AGE_0_CONTROL_FILE), cp(AGE_JSON_FILE));
    serverControl.get(0).deploy(cp(AGE_11_CONTROL_FILE), cp(AGE_JSON_FILE));

    Ticket ticket = serverControl.get(0).loginSuperuser();

    // THEN
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      serverControl.get(0).getSerivceTestUtil()
          .queryService((queryService) -> queryService.asyncExecuteQuery(ticket, queryUuid,
              "select age, count() from age group by age order by count() desc", true,
              queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received", 10, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);

      List<Pair<Long, Long>> expected = new ArrayList<>();
      expected.add(new Pair<>(5L, 10L));
      expected.add(new Pair<>(3L, 6L));
      expected.add(new Pair<>(2L, 4L));
      expected.add(new Pair<>(1L, 2L));

      List<Pair<Long, Long>> actual = new ArrayList<>();
      for (List<RValue> row : queryRes.getFinalUpdate().getRows()) {
        Assert.assertEquals(row.size(), 2, "Expected to get correct number of cols returned.");
        actual.add(new Pair<>((Long) RValueUtil.createValue(row.get(0)), (Long) RValueUtil.createValue(row.get(1))));
      }

      Assert.assertEquals(actual, expected, "Expected to get correct results.");
    } catch (IOException e) {
      throw new RuntimeException("Could not execute query", e);
    }

    // WHEN undeploy one of the shards
    serverControl.get(0).undeploy(cp(AGE_11_CONTROL_FILE));

    // THEN
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      serverControl.get(0).getSerivceTestUtil()
          .queryService((queryService) -> queryService.asyncExecuteQuery(ticket, queryUuid,
              "select age, count() from age group by age order by count() desc", true,
              queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received", 10, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);

      List<Pair<Long, Long>> expected = new ArrayList<>();
      expected.add(new Pair<>(5L, 5L));
      expected.add(new Pair<>(3L, 3L));
      expected.add(new Pair<>(2L, 2L));
      expected.add(new Pair<>(1L, 1L));

      List<Pair<Long, Long>> actual = new ArrayList<>();
      for (List<RValue> row : queryRes.getFinalUpdate().getRows()) {
        Assert.assertEquals(row.size(), 2, "Expected to get correct number of cols returned.");
        actual.add(new Pair<>((Long) RValueUtil.createValue(row.get(0)), (Long) RValueUtil.createValue(row.get(1))));
      }

      Assert.assertEquals(actual, expected, "Expected to get correct results.");
    } catch (IOException e) {
      throw new RuntimeException("Could not execute query", e);
    }
  }

  @Test(expectedExceptions = WaitTimeoutException.class)
  @NeedsServer(servers = 1)
  public void singleServerOverlappingShards() throws InterruptedException {
    // GIVEN
    try {
      serverControl.get(0).deploy(cp(AGE_0_CONTROL_FILE), cp(AGE_JSON_FILE));
    } catch (WaitTimeoutException e) {
      Assert.fail("WaitTimeoutException happened in the wrong place: " + e.toString());
    }

    // WHEN deploy control file with overlapping rowIds
    serverControl.get(0).deploy(cp(AGE_10_CONTROL_FILE), cp(AGE_JSON_FILE));

    // THEN WaitTimeoutException is thrown as the control file cannot be deployed.
  }

}
