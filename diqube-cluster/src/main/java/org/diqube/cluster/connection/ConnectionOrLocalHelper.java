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

import javax.inject.Inject;

import org.apache.thrift.TServiceClient;
import org.diqube.cluster.ClusterManager;
import org.diqube.context.AutoInstatiate;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.springframework.context.ApplicationContext;

/**
 * Helper class for getting services either by remote connection or locally if possible.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ConnectionOrLocalHelper {
  @Inject
  private ConnectionPool connectionPool;

  @Inject
  private ClusterManager clusterManager;

  @Inject
  private ApplicationContext beanContext;

  /**
   * Retrieve an instance of the given service for the given address and return a local bean from the bean context if
   * the given address is this nodes address.
   * 
   * @param thriftClientClass
   *          See {@link ConnectionPool#reserveConnection(Class, String, RNodeAddress, SocketListener)}
   * @param thriftServiceInterfaceClass
   *          The interface the thriftClientClass implements (and which the local bean implementing the service
   *          implement).
   * @param thriftServiceName
   *          See {@link ConnectionPool#reserveConnection(Class, String, RNodeAddress, SocketListener)}
   * @param addr
   *          See {@link ConnectionPool#reserveConnection(Class, String, RNodeAddress, SocketListener)}
   * @param socketListener
   *          See {@link ConnectionPool#reserveConnection(Class, String, RNodeAddress, SocketListener)}
   * @return A {@link ServiceProvider} that can be used to get the service bean (either local or remote access). Be sure
   *         to call {@link ServiceProvider#close()} when done!
   * @throws ConnectionException
   *           See {@link ConnectionPool#reserveConnection(Class, String, RNodeAddress, SocketListener)}. Additionally
   *           this is thrown if the local bean cannot be found.
   * @throws InterruptedException
   *           See {@link ConnectionPool#reserveConnection(Class, String, RNodeAddress, SocketListener)}
   */
  @SuppressWarnings("unchecked")
  public <T extends TServiceClient, U> ServiceProvider<U> getService(Class<T> thriftClientClass,
      Class<U> thriftServiceInterfaceClass, String thriftServiceName, RNodeAddress addr, SocketListener socketListener)
          throws ConnectionException, InterruptedException {
    if (clusterManager.getOurHostAddr().createRemote().equals(addr)) {
      // Thrift client class implements the service interface.
      U bean = beanContext.getBean(thriftServiceInterfaceClass);

      if (bean == null)
        throw new ConnectionException("Cannot find local instance of " + thriftServiceInterfaceClass.getName());

      return new LocalServiceProvider<U>(bean);
    } else {
      return (ServiceProvider<U>) connectionPool.reserveConnection(thriftClientClass, thriftServiceName, addr,
          socketListener);
    }
  }

  private static class LocalServiceProvider<T> implements ServiceProvider<T> {

    private T serviceBean;

    /* package */ LocalServiceProvider(T serviceBean) {
      this.serviceBean = serviceBean;
    }

    @Override
    public T getService() throws IllegalStateException {
      return serviceBean;
    }

    @Override
    public void close() throws IOException {
      // noop.
    }

    @Override
    public boolean isLocal() {
      return true;
    }
  }
}