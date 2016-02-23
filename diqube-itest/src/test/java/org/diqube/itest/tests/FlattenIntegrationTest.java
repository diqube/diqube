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
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.diqube.itest.AbstractDiqubeIntegrationTest;
import org.diqube.itest.annotations.NeedsServer;
import org.diqube.itest.util.QueryResultServiceTestUtil;
import org.diqube.itest.util.QueryResultServiceTestUtil.TestQueryResultService;
import org.diqube.itest.util.Waiter;
import org.diqube.server.ControlFileManager;
import org.diqube.thrift.base.thrift.RUUID;
import org.diqube.thrift.base.thrift.RValue;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.thrift.base.util.RUuidUtil;
import org.diqube.thrift.base.util.RValueUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Integration test which flattens data, requests it, adjusts the original table by loading more shards and then
 * re-flattens and re-requests.
 *
 * @author Bastian Gloeckle
 */
public class FlattenIntegrationTest extends AbstractDiqubeIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(FlattenIntegrationTest.class);

  private static final String TABLE = "flattendata";
  private static final String DATA0_JSON_FILE =
      "/" + FlattenIntegrationTest.class.getSimpleName() + "/flattendata0.json";
  private static final String DATA0_CONTROL_FILE =
      "/" + FlattenIntegrationTest.class.getSimpleName() + "/flattendata0" + ControlFileManager.CONTROL_FILE_EXTENSION;
  private static final String DATA3_JSON_FILE =
      "/" + FlattenIntegrationTest.class.getSimpleName() + "/flattendata3.json";
  private static final String DATA3_CONTROL_FILE =
      "/" + FlattenIntegrationTest.class.getSimpleName() + "/flattendata3" + ControlFileManager.CONTROL_FILE_EXTENSION;
  private static final String DATA6_JSON_FILE =
      "/" + FlattenIntegrationTest.class.getSimpleName() + "/flattendata6.json";
  private static final String DATA6_CONTROL_FILE =
      "/" + FlattenIntegrationTest.class.getSimpleName() + "/flattendata6" + ControlFileManager.CONTROL_FILE_EXTENSION;

  @Test
  @NeedsServer(servers = 2)
  public void flattenTest() throws IOException {
    // GIVEN
    serverControl.get(0).deploy(cp(DATA0_CONTROL_FILE), cp(DATA0_JSON_FILE));
    serverControl.get(1).deploy(cp(DATA6_CONTROL_FILE), cp(DATA6_JSON_FILE));

    Ticket ticket = serverControl.get(0).loginSuperuser();

    // WHEN: query
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      serverControl.get(0).getSerivceTestUtil()
          .queryService((queryService) -> queryService.asyncExecuteQuery(ticket, queryUuid,
              "select a.b, count() from flatten(" + TABLE + ", a[*]) group by a.b order by a.b asc", true,
              queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received", 10, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);

      // THEN: valid result.
      Assert.assertTrue(queryRes.getFinalUpdate().isSetRows(), "Expected to get a result");

      List<List<Long>> expectedResult = new ArrayList<>();
      expectedResult.add(Arrays.asList(1L, 3L)); // a.b = 1, count = 3
      expectedResult.add(Arrays.asList(2L, 3L)); // a.b = 2, count = 3
      expectedResult.add(Arrays.asList(5L, 3L)); // a.b = 5, count = 3
      expectedResult.add(Arrays.asList(6L, 3L)); // a.b = 6, count = 3

      List<List<Long>> actual = transformResult(queryRes.getFinalUpdate().getRows());

      Assert.assertEquals(actual, expectedResult, "Expected to get correct result BEFORE adding additional shard.");
    } catch (IOException e) {
      throw new RuntimeException("Could not execute query", e);
    }

    // WHEN: deploy another shard in the middle -> flattened data should be invalidated and re-calculated, since
    // otherwise rowIds would overlap.
    serverControl.get(0).deploy(cp(DATA3_CONTROL_FILE), cp(DATA3_JSON_FILE));

    // THEN: query again, receive updated results.
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      serverControl.get(0).getSerivceTestUtil()
          .queryService((queryService) -> queryService.asyncExecuteQuery(ticket, queryUuid,
              "select a.b, count() from flatten(" + TABLE + ", a[*]) group by a.b order by a.b asc", true,
              queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received", 10, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);

      // THEN: valid result.
      Assert.assertTrue(queryRes.getFinalUpdate().isSetRows(), "Expected to get a result");

      List<List<Long>> expectedResult = new ArrayList<>();
      expectedResult.add(Arrays.asList(1L, 3L)); // a.b = 1, count = 3
      expectedResult.add(Arrays.asList(2L, 3L)); // a.b = 2, count = 3
      expectedResult.add(Arrays.asList(3L, 3L)); // a.b = 3, count = 3
      expectedResult.add(Arrays.asList(4L, 3L)); // a.b = 4, count = 3
      expectedResult.add(Arrays.asList(5L, 3L)); // a.b = 5, count = 3
      expectedResult.add(Arrays.asList(6L, 3L)); // a.b = 6, count = 3

      List<List<Long>> actual = transformResult(queryRes.getFinalUpdate().getRows());

      Assert.assertEquals(actual, expectedResult, "Expected to get correct result AFTER adding additional shard.");
    } catch (IOException e) {
      throw new RuntimeException("Could not execute query", e);
    }
  }

  private List<List<Long>> transformResult(List<List<RValue>> in) {
    List<List<Long>> res = new ArrayList<>();
    for (List<RValue> inRow : in)
      res.add(inRow.stream().map(rvalue -> (Long) RValueUtil.createValue(rvalue)).collect(Collectors.toList()));

    return res;
  }

}
