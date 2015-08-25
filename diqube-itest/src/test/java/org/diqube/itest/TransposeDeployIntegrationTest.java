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
package org.diqube.itest;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.diqube.function.aggregate.ConcatGroupFunction;
import org.diqube.itest.annotations.NeedsServer;
import org.diqube.itest.control.TransposeControl;
import org.diqube.itest.util.QueryResultServiceTestUtil;
import org.diqube.itest.util.QueryResultServiceTestUtil.TestQueryResultService;
import org.diqube.itest.util.ServiceTestUtil;
import org.diqube.itest.util.Waiter;
import org.diqube.remote.base.thrift.RUUID;
import org.diqube.remote.base.thrift.RValue;
import org.diqube.remote.base.util.RUuidUtil;
import org.diqube.remote.base.util.RValueUtil;
import org.diqube.server.NewDataWatcher;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Integration test which first transposes some data and then imports that data into a server.
 *
 * @author Bastian Gloeckle
 */
public class TransposeDeployIntegrationTest extends AbstractDiqubeIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(TransposeDeployIntegrationTest.class);

  private static final String LOREM_JSON_TABLE = "loremJson";
  private static final String LOREM_DIQUBE_TABLE = "loremDiqube";
  private static final String LOREM_JSON_FILE =
      "/" + TransposeDeployIntegrationTest.class.getSimpleName() + "/lorem.json";
  private static final String LOREM_JSON_CONTROL_FILE =
      "/" + TransposeDeployIntegrationTest.class.getSimpleName() + "/loremJson" + NewDataWatcher.CONTROL_FILE_EXTENSION;
  private static final String LOREM_DIQUBE_CONTROL_FILE = "/" + TransposeDeployIntegrationTest.class.getSimpleName()
      + "/loremDiqube" + NewDataWatcher.CONTROL_FILE_EXTENSION;
  private static final String LOREM_DIQUBE_FILE = "lorem.diqube";

  @Test
  @NeedsServer(servers = 1)
  public void jsonTransposeDeploy() throws IOException {
    // GIVEN
    // transposed file
    transposeControl.transpose(cp(LOREM_JSON_FILE), TransposeControl.TYPE_JSON, work(LOREM_DIQUBE_FILE));

    // deploy both non-transposed and transposed data
    serverControl.get(0).deploy(cp(LOREM_JSON_CONTROL_FILE), cp(LOREM_JSON_FILE));
    serverControl.get(0).deploy(cp(LOREM_DIQUBE_CONTROL_FILE), work(LOREM_DIQUBE_FILE));

    Set<Pair<String, Set<String>>> expected = new HashSet<>();
    expected.add(new Pair<>("aute", new HashSet<>(Arrays.asList("eiusmod", "aliquip", "ex"))));
    expected.add(new Pair<>("dolor", new HashSet<>(Arrays.asList("cupidatat", "esse", "proident"))));
    expected.add(new Pair<>("voluptate", new HashSet<>(Arrays.asList("elit", "in", "magna"))));
    expected.add(new Pair<>("sunt", new HashSet<>(Arrays.asList("anim", "non", "cillum"))));
    expected.add(new Pair<>("elit", new HashSet<>(Arrays.asList("Lorem", "elit", "deserunt"))));
    expected.add(new Pair<>("do", new HashSet<>(Arrays.asList("aliquip", "do", "commodo"))));
    expected.add(new Pair<>("mollit", new HashSet<>(Arrays.asList("pariatur", "incididunt", "cillum"))));

    Set<Pair<String, Set<String>>> jsonResult = queryResults(LOREM_JSON_TABLE);
    Assert.assertEquals(jsonResult, expected);

    Set<Pair<String, Set<String>>> diqubeResult = queryResults(LOREM_DIQUBE_TABLE);
    Assert.assertEquals(diqubeResult, expected);
  }

  private Set<Pair<String, Set<String>>> queryResults(String tableName) throws IOException {
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      ServiceTestUtil.queryService(serverControl.get(0), (queryService) -> queryService.asyncExecuteQuery(queryUuid,
          "select a, concatgroup(b[*].c) from " + tableName, true, queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received", 10, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);

      Set<Pair<String, Set<String>>> result = new HashSet<>();
      for (List<RValue> row : queryRes.getFinalUpdate().getRows()) {
        Assert.assertEquals(row.size(), 2, "Expected to get correct number of cols returned.");
        result.add(new Pair<>((String) RValueUtil.createValue(row.get(0)), new HashSet<>(Arrays
            .asList(((String) RValueUtil.createValue(row.get(1))).split(ConcatGroupFunction.DEFAULT_DELIMITER)))));
      }
      return result;
    }
  }
}
