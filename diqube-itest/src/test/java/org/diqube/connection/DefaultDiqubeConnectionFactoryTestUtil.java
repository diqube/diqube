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

import org.diqube.connection.integrity.IntegritySecretHelper;
import org.diqube.connection.integrity.IntegritySecretHelperTestUtil;
import org.diqube.thrift.base.services.DiqubeThriftServiceInfoManager.DiqubeThriftServiceInfo;
import org.diqube.thrift.base.thrift.RNodeAddress;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Connection factory for itests. All returned connections are "unpooled", since the tests do not want to pool the conns
 * themselves.
 *
 * @author Bastian Gloeckle
 */
public class DefaultDiqubeConnectionFactoryTestUtil {

  public static ConnectionFactory createDefaultConnectionFactory(String macKey) {
    ConnectionPool pool = Mockito.mock(ConnectionPool.class);

    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        Connection<?> conn = (Connection<?>) invocation.getArguments()[0];
        conn.getTransport().close();
        return null;
      }
    }).when(pool).releaseConnection(Mockito.any());

    IntegritySecretHelper integritySecretHelper = new IntegritySecretHelper();
    IntegritySecretHelperTestUtil.setMessageIntegritySecret(integritySecretHelper, macKey);

    return new UnpoolingConnectionFactory(new DefaultConnectionFactory(pool, integritySecretHelper, 10000));
  }

  public static class UnpoolingConnectionFactory implements ConnectionFactory {
    private ConnectionFactory delegate;

    public UnpoolingConnectionFactory(ConnectionFactory delegate) {
      this.delegate = delegate;
    }

    @Override
    public <T> Connection<T> createConnection(DiqubeThriftServiceInfo<T> serviceInfo, RNodeAddress addr,
        SocketListener socketListener) throws ConnectionException {
      Connection<T> res = delegate.createConnection(serviceInfo, addr, socketListener);
      // unpool
      res.pooledCAS(true, false);
      return res;
    }

    @Override
    public <T, U> Connection<U> createConnection(Connection<T> oldConnection, DiqubeThriftServiceInfo<U> serviceInfo)
        throws ConnectionException {
      Connection<U> res = delegate.createConnection(oldConnection, serviceInfo);
      // unpool
      res.pooledCAS(true, false);
      return res;
    }
  }
}
