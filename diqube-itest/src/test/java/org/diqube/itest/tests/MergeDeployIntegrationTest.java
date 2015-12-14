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
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.diqube.itest.AbstractDiqubeIntegrationTest;
import org.diqube.itest.annotations.NeedsServer;
import org.diqube.itest.control.ToolControl;
import org.diqube.itest.util.QueryResultServiceTestUtil;
import org.diqube.itest.util.QueryResultServiceTestUtil.TestQueryResultService;
import org.diqube.itest.util.Waiter;
import org.diqube.server.NewDataWatcher;
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
 * Integration test which first transposes and merges some data and then imports that data into a server.
 *
 * @author Bastian Gloeckle
 */
public class MergeDeployIntegrationTest extends AbstractDiqubeIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(MergeDeployIntegrationTest.class);

  private static final String AGE_TABLE = "age";
  private static final String AGE_JSON_FILE = "/" + MergeDeployIntegrationTest.class.getSimpleName() + "/age.json";
  private static final String AGE_CONTROL_FILE =
      "/" + MergeDeployIntegrationTest.class.getSimpleName() + "/age_rowId0" + NewDataWatcher.CONTROL_FILE_EXTENSION;
  private static final String AGE_DIQUBE_PART1_FILE = "age1.diqube";
  private static final String AGE_DIQUBE_PART2_FILE = "age2.diqube";
  private static final String AGE_DIQUBE_MERGED_FILE = "age_merged.diqube";

  @Test
  @NeedsServer(servers = 1)
  public void mergeDeploy() throws IOException {
    // GIVEN
    // transposed file
    toolControl.transpose(cp(AGE_JSON_FILE), ToolControl.TYPE_JSON, work(AGE_DIQUBE_PART1_FILE));
    Files.copy(work(AGE_DIQUBE_PART1_FILE).toPath(), work(AGE_DIQUBE_PART2_FILE).toPath());
    toolControl.merge(Arrays.asList(work(AGE_DIQUBE_PART1_FILE), work(AGE_DIQUBE_PART2_FILE)),
        work(AGE_DIQUBE_MERGED_FILE));

    // deploy both non-transposed and transposed data
    serverControl.get(0).deploy(cp(AGE_CONTROL_FILE), work(AGE_DIQUBE_MERGED_FILE));

    Ticket ticket = serverControl.get(0).loginSuperuser();

    Set<Pair<Long, Long>> expected = new HashSet<>();
    expected.add(new Pair<>(5L, 10L));
    expected.add(new Pair<>(3L, 6L));
    expected.add(new Pair<>(2L, 4L));
    expected.add(new Pair<>(1L, 2L));

    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      serverControl.get(0).getSerivceTestUtil()
          .queryService((queryService) -> queryService.asyncExecuteQuery(ticket, queryUuid,
              "select age, count() from " + AGE_TABLE + " group by age", true,
              queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received", 10, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);

      Set<Pair<Long, Long>> actualResult = new HashSet<>();
      for (List<RValue> row : queryRes.getFinalUpdate().getRows()) {
        Assert.assertEquals(row.size(), 2, "Expected to get correct number of cols returned.");
        actualResult
            .add(new Pair<>((Long) RValueUtil.createValue(row.get(0)), (Long) RValueUtil.createValue(row.get(1))));
      }

      Assert.assertEquals(actualResult, expected, "Expected correct results");
    }

  }

}
