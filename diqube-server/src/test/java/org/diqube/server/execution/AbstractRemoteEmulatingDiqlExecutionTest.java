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
package org.diqube.server.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.diqube.connection.Connection;
import org.diqube.connection.ConnectionException;
import org.diqube.connection.ConnectionFactory;
import org.diqube.connection.ConnectionPool;
import org.diqube.connection.ConnectionPoolTestUtil;
import org.diqube.connection.SocketListener;
import org.diqube.data.column.ColumnType;
import org.diqube.queries.QueryRegistry;
import org.diqube.remote.cluster.thrift.ClusterQueryService;
import org.diqube.server.execution.util.NoopClusterQueryService;
import org.diqube.server.queryremote.query.ClusterQueryServiceHandler;
import org.diqube.testutil.TestContextOverrideBean;
import org.diqube.thrift.base.services.DiqubeThriftServiceInfoManager;
import org.diqube.thrift.base.services.DiqubeThriftServiceInfoManager.DiqubeThriftServiceInfo;
import org.diqube.thrift.base.thrift.RNodeAddress;
import org.diqube.util.Pair;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.annotations.BeforeMethod;

/**
 * Abstract base class for tests that want to mock away the remote.
 * 
 * @author Bastian Gloeckle
 */
public abstract class AbstractRemoteEmulatingDiqlExecutionTest<T> extends AbstractCacheDoubleDiqlExecutionTest<T> {

  protected QueryRegistry queryRegistry;

  public AbstractRemoteEmulatingDiqlExecutionTest(ColumnType colType, TestDataProvider<T> dp) {
    super(colType, dp);
  }

  @Override
  protected void adjustContextBeforeRefresh(AnnotationConfigApplicationContext ctx) {
    // replace the default ClusterQueryServiceHandler with a Noop handler, as otherwise the ExecuteRemote step would
    // access the only "remote" (= local process) through the bean in the context directly, without accessing any
    // connections - we would then not be able to fully emulate the remotes.
    TestContextOverrideBean.overrideBeanClass(ctx, ClusterQueryServiceHandler.class, NoopClusterQueryService.class);
  }

  @Override
  @BeforeMethod
  public void setUp() {
    super.setUp();

    queryRegistry = dataContext.getBean(QueryRegistry.class);

    // Let the connectionPool return mocks.
    ConnectionPool pool = dataContext.getBean(ConnectionPool.class);
    ConnectionPoolTestUtil.setConnectionFactory(pool, new ConnectionFactory() {
      @SuppressWarnings("unchecked")
      @Override
      public <C> Connection<C> createConnection(DiqubeThriftServiceInfo<C> serviceInfo, RNodeAddress addr,
          SocketListener socketListener) throws ConnectionException {
        return (Connection<C>) ConnectionPoolTestUtil.createConnection(pool,
            dataContext.getBean(DiqubeThriftServiceInfoManager.class).getServiceInfo(ClusterQueryService.Iface.class));
      }

      @Override
      public <U, V> Connection<V> createConnection(Connection<U> oldConnection, DiqubeThriftServiceInfo<V> serviceInfo)
          throws ConnectionException {
        return null;
      }
    });
  }

  /**
   * Initialize a number of sample shards so the test table has any shards. Returns a list with the 'first row ID' of
   * each created shard.
   */
  protected List<Long> initializeSampleTableShards(int numberOfShards) {
    List<Pair<Object[], Object[]>> shards = new ArrayList<>();
    for (int firstRowId = 0; firstRowId < numberOfShards; firstRowId++) {
      shards.add(new Pair<Object[], Object[]>(dp.a(firstRowId), dp.a(firstRowId)));
    }
    initializeMultiShardTable(shards);
    return LongStream.range(0, numberOfShards).mapToObj(Long::valueOf).collect(Collectors.toList());
  }

  /**
   * Wait until the {@link Object#notifyAll()} method is called on a specific object /and/ a specific testFunction
   * returns true.
   * 
   * <p>
   * This will wait some time and if after that the test function still fails, it will throw a {@link RuntimeException}
   * with a given message.
   */
  protected void waitUntilOrFail(Object objToWaitFor, Supplier<String> msgSupplier, Supplier<Boolean> testFunction)
      throws RuntimeException {
    int maxRuns = 50; // 5s.
    while (maxRuns-- > 0) {
      synchronized (objToWaitFor) {
        try {
          objToWaitFor.wait(100);
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted while waiting for result", e);
        }
      }

      if (testFunction.get())
        return;
    }
    throw new RuntimeException(msgSupplier.get());
  }

}
