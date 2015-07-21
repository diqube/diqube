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
package org.diqube.cluster.connection;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransport;
import org.diqube.cluster.ClusterManager;
import org.diqube.cluster.NodeAddress;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.cluster.thrift.ClusterManagementService;
import org.diqube.remote.query.thrift.KeepAliveService;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for {@link ConnectionPool}.
 *
 * @author Bastian Gloeckle
 */
public class ConnectionPoolTest {
  private static final RNodeAddress ADDR1 = new NodeAddress("localhost", (short) 5101).createRemote();
  private static final RNodeAddress ADDR2 = new NodeAddress("localhost", (short) 5102).createRemote();
  private static final RNodeAddress ADDR3 = new NodeAddress("localhost", (short) 5103).createRemote();

  private ConnectionPool pool;

  private TestConnectionFactory conFac;

  @BeforeMethod
  public void before() {
    conFac = new TestConnectionFactory();

    pool = new ConnectionPool();
    ClusterManager cmMock = Mockito.mock(ClusterManager.class, Mockito.RETURNS_MOCKS);
    // be sure to forward "nodeDied" calls
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        RNodeAddress nodeDied = (RNodeAddress) invocation.getArguments()[0];
        pool.nodeDied(nodeDied);
        return null;
      }
    }).when(cmMock).nodeDied(Mockito.any());
    pool.setClusterManager(cmMock);
  }

  @AfterMethod
  public void after() {
    pool.cleanup();
  }

  private void initPool(int keepAliveMs, int connectionSoftLimit, int connectionIdleTimeMs) {
    pool.setKeepAliveMs(keepAliveMs);
    pool.setConnectionSoftLimit(connectionSoftLimit);
    pool.setConnectionIdleTimeMs(connectionIdleTimeMs);
    pool.initialize();
    pool.setConnectionFactory(conFac);
  }

  @Test
  public void connectionReuse() throws ConnectionException, InterruptedException, IOException {
    initPool(Integer.MAX_VALUE, 2, Integer.MAX_VALUE);

    TestConnection<ClusterManagementService.Client> conn = (TestConnection<ClusterManagementService.Client>) pool
        .reserveConnection(ClusterManagementService.Client.class, null, ADDR1, null);

    int connId = conn.getId();
    Assert.assertEquals(conn.getServiceClientClass(), ClusterManagementService.Client.class,
        "Correct service expected");
    conn.close(); // release connection

    // re-request address
    conn = (TestConnection<ClusterManagementService.Client>) pool
        .reserveConnection(ClusterManagementService.Client.class, null, ADDR1, null);
    conn.close();

    Assert.assertEquals(conn.getId(), connId, "Expected that connection is re-used");
    Assert.assertEquals(conn.getServiceClientClass(), ClusterManagementService.Client.class,
        "Correct service expected");
  }

  @Test
  public void blockTimeoutNewConnection() throws ConnectionException, InterruptedException, IOException {
    initPool(Integer.MAX_VALUE, //
        1, // Only 1 connection simultaneously - we expect to block!
        3_000 // 3s idle time.
    );

    TestConnection<ClusterManagementService.Client> conn = (TestConnection<ClusterManagementService.Client>) pool
        .reserveConnection(ClusterManagementService.Client.class, null, ADDR1, null);

    int connId = conn.getId();
    Assert.assertEquals(conn.getServiceClientClass(), ClusterManagementService.Client.class,
        "Correct service expected");
    long beforeNanos = System.nanoTime();
    conn.close(); // release connection

    // request a connection to another host (so it won't get re-used), which should be blocked - the first connection
    // was released, but it is effectively still open until it is closed automatically by the timeout!
    TestConnection<ClusterManagementService.Client> conn2 = (TestConnection<ClusterManagementService.Client>) pool
        .reserveConnection(ClusterManagementService.Client.class, null, ADDR2, null);
    long afterNanos = System.nanoTime();
    conn2.close();

    // ensure that the "close" method on the first connection was called
    Mockito.verify(conn.getTransport()).close();

    Assert.assertTrue(afterNanos - beforeNanos >= 3_000_000_000L,
        "Expected to have blocked at least the timeout time until we receive a new connection, but waited only "
            + (afterNanos - beforeNanos) + " nanos");
    Assert.assertNotEquals(conn2.getId(), connId, "Expected that connection was not re-used, as first "
        + "connection should have been closed before the second was opened");
    Assert.assertEquals(conn2.getServiceClientClass(), ClusterManagementService.Client.class,
        "Correct service expected");
  }

  @Test
  public void blockKeepAliveDeadNewConnection() throws ConnectionException, InterruptedException, IOException {
    initPool(1_000, // 1s keep alive - check approx every second for keep alives.
        1, // Only 1 connection simultaneously - we expect to block!
        Integer.MAX_VALUE);

    TestConnection<ClusterManagementService.Client> conn = (TestConnection<ClusterManagementService.Client>) pool
        .reserveConnection(ClusterManagementService.Client.class, null, ADDR1, null);

    int connId = conn.getId();
    Assert.assertEquals(conn.getServiceClientClass(), ClusterManagementService.Client.class,
        "Correct service expected");
    long beforeNanos = System.nanoTime();

    // adjust the morph of the connection to a KeepAliveService and make sure there is an exception thrown when the
    // "ping" method is called.
    conFac.adjustMorphOfConn(connId, new Consumer<TestConnection<? extends TServiceClient>>() {
      @Override
      public void accept(TestConnection<? extends TServiceClient> t) {
        try {
          if (t.getService() instanceof KeepAliveService.Iface)
            Mockito.doThrow(TException.class).when(((KeepAliveService.Iface) t.getService())).ping();
        } catch (TException e) {
          throw new RuntimeException(e);
        }
      }
    });
    conn.close(); // release connection

    // request a connection to another host (so it won't get re-used), which should be blocked - the first connection
    // was released, but it is effectively still open until it is closed automatically by the timeout!
    TestConnection<ClusterManagementService.Client> conn2 = (TestConnection<ClusterManagementService.Client>) pool
        .reserveConnection(ClusterManagementService.Client.class, null, ADDR2, null);
    long afterNanos = System.nanoTime();
    conn2.close();

    // ensure that the "close" method on the first connection was called
    Mockito.verify(conn.getTransport()).close();

    Assert.assertTrue(afterNanos - beforeNanos >= 1_000_000_000L,
        "Expected to have blocked at least the keep-alive time until it was found that the first conn is dead and a "
            + "new conn was opened, but waited only " + (afterNanos - beforeNanos) + " nanos");
    Assert.assertNotEquals(conn2.getId(), connId, "Expected that connection was not re-used, as "
        + "first connection should have been closed before the second was opened");
    Assert.assertEquals(conn2.getServiceClientClass(), ClusterManagementService.Client.class,
        "Correct service expected");
  }

  @Test
  public void reuseDeadNewConnection() throws ConnectionException, InterruptedException, IOException {
    initPool(Integer.MAX_VALUE, // high keep-alive so it won't get in the way...
        1, //
        Integer.MAX_VALUE);

    TestConnection<ClusterManagementService.Client> conn = (TestConnection<ClusterManagementService.Client>) pool
        .reserveConnection(ClusterManagementService.Client.class, null, ADDR1, null);

    int connId = conn.getId();
    Assert.assertEquals(conn.getServiceClientClass(), ClusterManagementService.Client.class,
        "Correct service expected");

    // adjust the morph of the connection to a KeepAliveService and make sure there is an exception thrown when the
    // "ping" method is called - this should be executed before re-using the connection above - marking the connection
    // above as dead.
    conFac.adjustMorphOfConn(connId, new Consumer<TestConnection<? extends TServiceClient>>() {
      @Override
      public void accept(TestConnection<? extends TServiceClient> t) {
        try {
          if (t.getService() instanceof KeepAliveService.Iface)
            Mockito.doThrow(TException.class).when(((KeepAliveService.Iface) t.getService())).ping();
        } catch (TException e) {
          throw new RuntimeException(e);
        }
      }
    });
    conn.close(); // release connection

    // request a connection to the same host (so it will get re-used), which should be blocked - the first connection
    // was released, but it is effectively still open until it is closed automatically by the timeout!
    TestConnection<ClusterManagementService.Client> conn2 = (TestConnection<ClusterManagementService.Client>) pool
        .reserveConnection(ClusterManagementService.Client.class, null, ADDR1, null);
    conn2.close();

    // ensure that the "close" method on the first connection was called
    Mockito.verify(conn.getTransport()).close();

    Assert.assertNotEquals(conn2.getId(), connId, "Expected that connection was not re-used, as "
        + "first connection was tried to be re-used but appeared to have died");
    Assert.assertEquals(conn2.getServiceClientClass(), ClusterManagementService.Client.class,
        "Correct service expected");
  }

  @Test
  public void nodeDiedOnConnection() throws ConnectionException, InterruptedException, IOException {
    initPool(Integer.MAX_VALUE, // high keep-alive so it won't get in the way...
        1, //
        Integer.MAX_VALUE);

    TestConnection<ClusterManagementService.Client> conn = (TestConnection<ClusterManagementService.Client>) pool
        .reserveConnection(ClusterManagementService.Client.class, null, ADDR1, null);

    int connId = conn.getId();
    Assert.assertEquals(conn.getServiceClientClass(), ClusterManagementService.Client.class,
        "Correct service expected");
    conn.close(); // release connection

    // simulate that some other part of diqube identified ADDR1 to be down.
    pool.nodeDied(ADDR1);

    // request another connection, which should open a new one!
    TestConnection<ClusterManagementService.Client> conn2 = (TestConnection<ClusterManagementService.Client>) pool
        .reserveConnection(ClusterManagementService.Client.class, null, ADDR1, null);
    conn2.close();

    // ensure that the "close" method on the first connection was called
    Mockito.verify(conn.getTransport()).close();

    Assert.assertNotEquals(conn2.getId(), connId,
        "Expected that connection was not re-used, as " + "node died in between");
    Assert.assertEquals(conn2.getServiceClientClass(), ClusterManagementService.Client.class,
        "Correct service expected");
  }

  /**
   * A {@link ConnectionFactory} used in this test.
   */
  private class TestConnectionFactory implements ConnectionFactory {
    private AtomicInteger nextId = new AtomicInteger(0);

    private Map<Integer, Consumer<TestConnection<? extends TServiceClient>>> morphConsumers = new HashMap<>();

    public void adjustMorphOfConn(int connId, Consumer<TestConnection<? extends TServiceClient>> adjuster) {
      morphConsumers.put(connId, adjuster);
    }

    @Override
    public <T extends TServiceClient, U extends TServiceClient> Connection<U> createConnection(
        Connection<T> oldConnection, Class<U> newThriftClientClass, String newThriftServiceName)
            throws ConnectionException {
      TestConnection<U> res = new TestConnection<>(newThriftClientClass, ((TestConnection<?>) oldConnection).getId(),
          ((TestConnection<?>) oldConnection).getAddress(), ((TestConnection<?>) oldConnection).getTransport());
      res.setTimeout(oldConnection.getTimeout());
      res.setExecutionUuid(oldConnection.getExecutionUuid());
      if (morphConsumers.containsKey(res.getId()))
        morphConsumers.get(res.getId()).accept(res);
      return res;
    }

    @Override
    public <T extends TServiceClient> Connection<T> createConnection(Class<T> thriftClientClass,
        String thriftServiceName, RNodeAddress addr, SocketListener socketListener) throws ConnectionException {
      return new TestConnection<>(thriftClientClass, nextId.getAndIncrement(), addr);
    }
  }

  /**
   * {@link Connection} class used in test.
   */
  private class TestConnection<T> extends Connection<T> {
    private int id;

    TestConnection(Class<T> serviceClientClass, int id, RNodeAddress addr, TTransport transport) {
      super(pool, serviceClientClass, Mockito.mock(serviceClientClass, Mockito.RETURNS_MOCKS), transport, addr);
      this.id = id;
    }

    TestConnection(Class<T> serviceClientClass, int id, RNodeAddress addr) {
      this(serviceClientClass, id, addr, Mockito.mock(TTransport.class));
    }

    public int getId() {
      return id;
    }

  }
}
