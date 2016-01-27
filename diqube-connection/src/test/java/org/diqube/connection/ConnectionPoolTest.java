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
package org.diqube.connection;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.diqube.connection.integrity.IntegritySecretHelper;
import org.diqube.connection.integrity.IntegritySecretHelperTestUtil;
import org.diqube.queries.QueryUuid;
import org.diqube.remote.cluster.thrift.ClusterManagementService;
import org.diqube.remote.query.thrift.KeepAliveService;
import org.diqube.thrift.base.services.DiqubeThriftServiceInfoManager;
import org.diqube.thrift.base.services.DiqubeThriftServiceInfoManager.DiqubeThriftServiceInfo;
import org.diqube.thrift.base.thrift.RNodeAddress;
import org.diqube.thrift.base.thrift.RNodeDefaultAddress;
import org.mockito.Mockito;
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
  private static final RNodeAddress ADDR1 = createDefaultRNodeAddress("localhost", (short) 5101);
  private static final RNodeAddress ADDR2 = createDefaultRNodeAddress("localhost", (short) 5102);

  private ConnectionPool pool;

  private TestConnectionFactory conFac;

  @BeforeMethod
  public void before() {
    conFac = new TestConnectionFactory();

    DiqubeThriftServiceInfoManager infoMgr = new DiqubeThriftServiceInfoManager();
    infoMgr.initialize();

    IntegritySecretHelper integritySecretHelper = new IntegritySecretHelper();
    IntegritySecretHelperTestUtil.setMessageIntegritySecret(integritySecretHelper, "abc");

    pool = new ConnectionPool();
    pool.setDiqubeThriftServiceInfoManager(infoMgr);
    pool.setIntegritySecretHelper(integritySecretHelper);
  }

  @AfterMethod
  public void after() {
    pool.cleanup();
  }

  private void initPool(int keepAliveMs, int connectionSoftLimit, int connectionIdleTimeMs, double earlyCloseLevel) {
    pool.setKeepAliveMs(keepAliveMs);
    pool.setConnectionSoftLimit(connectionSoftLimit);
    pool.setConnectionIdleTimeMs(connectionIdleTimeMs);
    pool.setEarlyCloseLevel(earlyCloseLevel);
    pool.initialize();
    pool.setConnectionFactory(conFac);
  }

  @Test
  public void connectionReuse() throws ConnectionException, InterruptedException, IOException {
    initPool(Integer.MAX_VALUE, 2, Integer.MAX_VALUE, .95);

    TestConnection<ClusterManagementService.Iface> conn = (TestConnection<ClusterManagementService.Iface>) pool
        .reserveConnection(ClusterManagementService.Iface.class, ADDR1, null);

    int connId = conn.getId();
    Assert.assertEquals(conn.getServiceInfo().getServiceInterface(), ClusterManagementService.Iface.class,
        "Correct service expected");
    conn.close(); // release connection

    // re-request address
    conn = (TestConnection<ClusterManagementService.Iface>) pool.reserveConnection(ClusterManagementService.Iface.class,
        ADDR1, null);
    conn.close();

    Assert.assertEquals(conn.getId(), connId, "Expected that connection is re-used");
    Assert.assertEquals(conn.getServiceInfo().getServiceInterface(), ClusterManagementService.Iface.class,
        "Correct service expected");
  }

  @Test
  public void connectionUsableAfterReserve() throws ConnectionException, InterruptedException, IOException {
    initPool(Integer.MAX_VALUE, 2, Integer.MAX_VALUE, .95);

    TestConnection<ClusterManagementService.Iface> conn = (TestConnection<ClusterManagementService.Iface>) pool
        .reserveConnection(ClusterManagementService.Iface.class, ADDR1, null);

    conn.getService();
    // expected: No exception
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void connectionUnusableAfterRelease() throws ConnectionException, InterruptedException, IOException {
    initPool(Integer.MAX_VALUE, 2, Integer.MAX_VALUE, .95);

    TestConnection<ClusterManagementService.Iface> conn = (TestConnection<ClusterManagementService.Iface>) pool
        .reserveConnection(ClusterManagementService.Iface.class, ADDR1, null);

    conn.getService();
    // expected: No exception

    pool.releaseConnection(conn);

    conn.getService();
  }

  @Test
  public void connectionReleaseMultipleTimes() throws ConnectionException, InterruptedException, IOException {
    initPool(Integer.MAX_VALUE, 2, Integer.MAX_VALUE, .95);

    TestConnection<ClusterManagementService.Iface> conn = (TestConnection<ClusterManagementService.Iface>) pool
        .reserveConnection(ClusterManagementService.Iface.class, ADDR1, null);

    conn.getService();

    pool.releaseConnection(conn);
    pool.releaseConnection(conn);
    // expected: No exception
  }

  @Test
  public void blockTimeoutNewConnectionHighEarlyCloseLevel()
      throws ConnectionException, InterruptedException, IOException {
    initPool(Integer.MAX_VALUE, //
        1, // Only 1 connection simultaneously - we expect to block!
        3_000, // 3s idle time.
        2); // high earlyCloseLevel to force to block

    TestConnection<ClusterManagementService.Iface> conn = (TestConnection<ClusterManagementService.Iface>) pool
        .reserveConnection(ClusterManagementService.Iface.class, ADDR1, null);

    int connId = conn.getId();
    Assert.assertEquals(conn.getServiceInfo().getServiceInterface(), ClusterManagementService.Iface.class,
        "Correct service expected");
    long beforeNanos = System.nanoTime();
    conn.close(); // release connection

    // request a connection to another host (so it won't get re-used), which should be blocked - the first connection
    // was released, but it is effectively still open until it is closed automatically by the timeout!
    TestConnection<ClusterManagementService.Iface> conn2 = (TestConnection<ClusterManagementService.Iface>) pool
        .reserveConnection(ClusterManagementService.Iface.class, ADDR2, null);
    long afterNanos = System.nanoTime();
    conn2.close();

    // ensure that the "close" method on the first connection was called
    Mockito.verify(conn.getTransport()).close();

    Assert.assertTrue(afterNanos - beforeNanos >= 3_000_000_000L,
        "Expected to have blocked at least the timeout time until we receive a new connection, but waited only "
            + (afterNanos - beforeNanos) + " nanos");
    Assert.assertNotEquals(conn2.getId(), connId, "Expected that connection was not re-used, as first "
        + "connection should have been closed before the second was opened");
    Assert.assertEquals(conn2.getServiceInfo().getServiceInterface(), ClusterManagementService.Iface.class,
        "Correct service expected");
  }

  @Test
  public void earlyCloseOfAvailableConnections() throws ConnectionException, InterruptedException, IOException {
    initPool(Integer.MAX_VALUE, //
        1, // Only 1 connection simultaneously
        Integer.MAX_VALUE, //
        .95);

    TestConnection<ClusterManagementService.Iface> conn = (TestConnection<ClusterManagementService.Iface>) pool
        .reserveConnection(ClusterManagementService.Iface.class, ADDR1, null);

    int connId = conn.getId();
    Assert.assertEquals(conn.getServiceInfo().getServiceInterface(), ClusterManagementService.Iface.class,
        "Correct service expected");
    conn.close(); // release connection

    // request a connection to another host (so it won't get re-used), which in our case should NOT be blocked, as the
    // pool should reach the "early close" level and close the first connection right away without waiting for it to
    // timeout.
    TestConnection<ClusterManagementService.Iface> conn2 = (TestConnection<ClusterManagementService.Iface>) pool
        .reserveConnection(ClusterManagementService.Iface.class, ADDR2, null);
    conn2.close();

    // ensure that the "close" method on the first connection was called
    Mockito.verify(conn.getTransport()).close();

    Assert.assertNotEquals(conn2.getId(), connId, "Expected that connection was not re-used, as first "
        + "connection should have been closed before the second was opened");
    Assert.assertEquals(conn2.getServiceInfo().getServiceInterface(), ClusterManagementService.Iface.class,
        "Correct service expected");
  }

  @Test
  public void blockKeepAliveDeadNewConnectionHighEarlyCloseLevel()
      throws ConnectionException, InterruptedException, IOException {
    initPool(1_000, // 1s keep alive - check approx every second for keep alives.
        1, // Only 1 connection simultaneously - we expect to block!
        Integer.MAX_VALUE, //
        2); // high earlyCloseLevel to force to block

    TestConnection<ClusterManagementService.Iface> conn = (TestConnection<ClusterManagementService.Iface>) pool
        .reserveConnection(ClusterManagementService.Iface.class, ADDR1, null);

    int connId = conn.getId();
    Assert.assertEquals(conn.getServiceInfo().getServiceInterface(), ClusterManagementService.Iface.class,
        "Correct service expected");
    long beforeNanos = System.nanoTime();

    // adjust the morph of the connection to a KeepAliveService and make sure there is an exception thrown when the
    // "ping" method is called.
    conFac.adjustMorphOfConn(connId, new Consumer<TestConnection<?>>() {
      @Override
      public void accept(TestConnection<?> t) {
        try {
          if (t.getServiceInfo().getServiceInterface().equals(KeepAliveService.Iface.class)) {
            // temp unpool to install our mock
            t.pooledCAS(true, false);
            Mockito.doThrow(TException.class).when(((KeepAliveService.Iface) t.getService())).ping();
            t.pooledCAS(false, true);
          }
        } catch (TException e) {
          throw new RuntimeException(e);
        }
      }
    });
    conn.close(); // release connection

    // request a connection to another host (so it won't get re-used), which should be blocked - the first connection
    // was released, but it is effectively still open until it is closed automatically by the timeout!
    TestConnection<ClusterManagementService.Iface> conn2 = (TestConnection<ClusterManagementService.Iface>) pool
        .reserveConnection(ClusterManagementService.Iface.class, ADDR2, null);
    long afterNanos = System.nanoTime();
    conn2.close();

    // ensure that the "close" method on the first connection was called
    Mockito.verify(conn.getTransport()).close();

    Assert.assertTrue(afterNanos - beforeNanos >= 1_000_000_000L,
        "Expected to have blocked at least the keep-alive time until it was found that the first conn is dead and a "
            + "new conn was opened, but waited only " + (afterNanos - beforeNanos) + " nanos");
    Assert.assertNotEquals(conn2.getId(), connId, "Expected that connection was not re-used, as "
        + "first connection should have been closed before the second was opened");
    Assert.assertEquals(conn2.getServiceInfo().getServiceInterface(), ClusterManagementService.Iface.class,
        "Correct service expected");
  }

  @Test
  public void reuseDeadNewConnection() throws ConnectionException, InterruptedException, IOException {
    initPool(Integer.MAX_VALUE, // high keep-alive so it won't get in the way...
        1, //
        Integer.MAX_VALUE, //
        .95);

    TestConnection<ClusterManagementService.Iface> conn = (TestConnection<ClusterManagementService.Iface>) pool
        .reserveConnection(ClusterManagementService.Iface.class, ADDR1, null);

    int connId = conn.getId();
    Assert.assertEquals(conn.getServiceInfo().getServiceInterface(), ClusterManagementService.Iface.class,
        "Correct service expected");

    // adjust the morph of the connection to a KeepAliveService and make sure there is an exception thrown when the
    // "ping" method is called - this should be executed before re-using the connection above - marking the connection
    // above as dead.
    conFac.adjustMorphOfConn(connId, new Consumer<TestConnection<?>>() {
      @Override
      public void accept(TestConnection<?> t) {
        try {
          if (t.getServiceInfo().getServiceInterface().equals(KeepAliveService.Iface.class)) {
            // temp unpool to install our mock
            t.pooledCAS(true, false);
            Mockito.doThrow(TException.class).when(((KeepAliveService.Iface) t.getService())).ping();
            t.pooledCAS(false, true);
          }
        } catch (TException e) {
          throw new RuntimeException(e);
        }
      }
    });
    conn.close(); // release connection

    // request a connection to the same host (so it will get re-used), which should be blocked - the first connection
    // was released, but it is effectively still open until it is closed automatically by the timeout!
    TestConnection<ClusterManagementService.Iface> conn2 = (TestConnection<ClusterManagementService.Iface>) pool
        .reserveConnection(ClusterManagementService.Iface.class, ADDR1, null);
    conn2.close();

    // ensure that the "close" method on the first connection was called
    Mockito.verify(conn.getTransport()).close();

    Assert.assertNotEquals(conn2.getId(), connId, "Expected that connection was not re-used, as "
        + "first connection was tried to be re-used but appeared to have died");
    Assert.assertEquals(conn2.getServiceInfo().getServiceInterface(), ClusterManagementService.Iface.class,
        "Correct service expected");
  }

  @Test
  public void nodeDiedOnConnection() throws ConnectionException, InterruptedException, IOException {
    initPool(Integer.MAX_VALUE, // high keep-alive so it won't get in the way...
        1, //
        Integer.MAX_VALUE, //
        .95);

    TestConnection<ClusterManagementService.Iface> conn = (TestConnection<ClusterManagementService.Iface>) pool
        .reserveConnection(ClusterManagementService.Iface.class, ADDR1, null);

    int connId = conn.getId();
    Assert.assertEquals(conn.getServiceInfo().getServiceInterface(), ClusterManagementService.Iface.class,
        "Correct service expected");
    conn.close(); // release connection

    // simulate that some other part of diqube identified ADDR1 to be down.
    pool.nodeDied(ADDR1);

    // request another connection, which should open a new one!
    TestConnection<ClusterManagementService.Iface> conn2 = (TestConnection<ClusterManagementService.Iface>) pool
        .reserveConnection(ClusterManagementService.Iface.class, ADDR1, null);
    conn2.close();

    // ensure that the "close" method on the first connection was called
    Mockito.verify(conn.getTransport()).close();

    Assert.assertNotEquals(conn2.getId(), connId,
        "Expected that connection was not re-used, as " + "node died in between");
    Assert.assertEquals(conn2.getServiceInfo().getServiceInterface(), ClusterManagementService.Iface.class,
        "Correct service expected");
  }

  @Test
  public void executionGetsAnotherConn() throws ConnectionException, InterruptedException, IOException {
    initPool(Integer.MAX_VALUE, // high keep-alive so it won't get in the way...
        1, // only one node, we don't want to block, though!
        Integer.MAX_VALUE, //
        .95);

    try {
      QueryUuid.setCurrentQueryUuidAndExecutionUuid(UUID.randomUUID(), UUID.randomUUID());
      TestConnection<ClusterManagementService.Iface> conn = (TestConnection<ClusterManagementService.Iface>) pool
          .reserveConnection(ClusterManagementService.Iface.class, ADDR1, null);

      int connId = conn.getId();
      Assert.assertEquals(conn.getServiceInfo().getServiceInterface(), ClusterManagementService.Iface.class,
          "Correct service expected");

      // request another connection to another node. We would break the limit, though we want that connection for the
      // same executionUuid, therefore we should get it without blocking.
      TestConnection<ClusterManagementService.Iface> conn2 = (TestConnection<ClusterManagementService.Iface>) pool
          .reserveConnection(ClusterManagementService.Iface.class, ADDR2, null);
      conn2.close();

      conn.close();

      Assert.assertNotEquals(conn2.getId(), connId,
          "Expected that connection was not re-used, as both connections should be open at the same time");
      Assert.assertEquals(conn2.getServiceInfo().getServiceInterface(), ClusterManagementService.Iface.class,
          "Correct service expected");
    } finally {
      QueryUuid.clearCurrent();
    }
  }

  /**
   * A {@link ConnectionFactory} used in this test.
   */
  private class TestConnectionFactory implements ConnectionFactory {
    private AtomicInteger nextId = new AtomicInteger(0);

    private Map<Integer, Consumer<TestConnection<?>>> morphConsumers = new HashMap<>();

    public void adjustMorphOfConn(int connId, Consumer<TestConnection<?>> adjuster) {
      morphConsumers.put(connId, adjuster);
    }

    @Override
    public <T, U> Connection<U> createConnection(Connection<T> oldConnection, DiqubeThriftServiceInfo<U> serviceInfo)
        throws ConnectionException {
      TestConnection<U> res = new TestConnection<>(serviceInfo, ((TestConnection<?>) oldConnection).getId(),
          ((TestConnection<?>) oldConnection).getAddress(), ((TestConnection<?>) oldConnection).getTransport());
      res.setTimeout(oldConnection.getTimeout());
      res.setExecutionUuid(oldConnection.getExecutionUuid());
      if (morphConsumers.containsKey(res.getId()))
        morphConsumers.get(res.getId()).accept(res);
      return res;
    }

    @Override
    public <T> Connection<T> createConnection(DiqubeThriftServiceInfo<T> serviceInfo, RNodeAddress addr,
        SocketListener socketListener) throws ConnectionException {
      return new TestConnection<>(serviceInfo, nextId.getAndIncrement(), addr);
    }
  }

  /**
   * {@link Connection} class used in test.
   */
  private class TestConnection<T> extends Connection<T> {
    private int id;

    TestConnection(DiqubeThriftServiceInfo<T> serviceInfo, int id, RNodeAddress addr, TTransport transport) {
      super(pool, serviceInfo, Mockito.mock(serviceInfo.getServiceInterface(), Mockito.RETURNS_MOCKS), transport, addr);
      this.id = id;
    }

    TestConnection(DiqubeThriftServiceInfo<T> serviceInfo, int id, RNodeAddress addr) {
      this(serviceInfo, id, addr, Mockito.mock(TTransport.class));
    }

    public int getId() {
      return id;
    }

  }

  private static RNodeAddress createDefaultRNodeAddress(String host, short port) {
    RNodeAddress res = new RNodeAddress();
    res.setDefaultAddr(new RNodeDefaultAddress());
    res.getDefaultAddr().setHost(host);
    res.getDefaultAddr().setPort(port);
    return res;
  }
}
