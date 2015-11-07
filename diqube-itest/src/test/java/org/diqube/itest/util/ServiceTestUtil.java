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
package org.diqube.itest.util;

import java.io.IOException;

import org.apache.thrift.TException;
import org.diqube.itest.control.ServerControl;
import org.diqube.itest.util.TestThriftConnectionFactory.TestConnection;
import org.diqube.itest.util.TestThriftConnectionFactory.TestConnectionException;
import org.diqube.remote.cluster.ClusterFlattenServiceConstants;
import org.diqube.remote.cluster.ClusterManagementServiceConstants;
import org.diqube.remote.cluster.thrift.ClusterFlattenService;
import org.diqube.remote.cluster.thrift.ClusterManagementService;
import org.diqube.remote.query.QueryServiceConstants;
import org.diqube.remote.query.thrift.QueryService;

/**
 * Utiltiy class to simply access the services of cluster servers in tests.
 *
 * @author Bastian Gloeckle
 */
public class ServiceTestUtil {
  /**
   * Open a connection to the {@link ClusterManagementService} of a specific node and then execute something.
   * 
   * @throws RuntimeException
   *           if anything goes wrong.
   */
  public static void clusterMgmtService(ServerControl server, RemoteConsumer<ClusterManagementService.Iface> execute) {
    try (TestConnection<ClusterManagementService.Client> con = TestThriftConnectionFactory.open(server.getAddr(),
        ClusterManagementService.Client.class, ClusterManagementServiceConstants.SERVICE_NAME)) {

      execute.accept(con.getService());

    } catch (IOException | TestConnectionException | TException e) {
      throw new RuntimeException("Exception while accessing ClusterManagementService of " + server.getAddr(), e);
    }
  }

  /**
   * Open a connection to the {@link QueryService} of a specific node and then execute something.
   * 
   * @throws RuntimeException
   *           if anything goes wrong.
   */
  public static void queryService(ServerControl server, RemoteConsumer<QueryService.Iface> execute) {
    try (TestConnection<QueryService.Client> con = TestThriftConnectionFactory.open(server.getAddr(),
        QueryService.Client.class, QueryServiceConstants.SERVICE_NAME)) {

      execute.accept(con.getService());

    } catch (IOException | TestConnectionException | TException e) {
      throw new RuntimeException("Exception while accessing QueryService of " + server.getAddr(), e);
    }
  }

  /**
   * Open a connection to the {@link ClusterFlattenService} of a specific node and then execute something.
   * 
   * @throws RuntimeException
   *           if anything goes wrong.
   */
  public static void clusterFlattenService(ServerControl server, RemoteConsumer<ClusterFlattenService.Iface> execute) {
    try (TestConnection<ClusterFlattenService.Client> con = TestThriftConnectionFactory.open(server.getAddr(),
        ClusterFlattenService.Client.class, ClusterFlattenServiceConstants.SERVICE_NAME)) {

      execute.accept(con.getService());

    } catch (IOException | TestConnectionException | TException e) {
      throw new RuntimeException("Exception while accessing ClusterFlattenService of " + server.getAddr(), e);
    }
  }

  /**
   * Simple consumer of a thrift (remote) service, with correct thrift exceptioning.
   */
  public static interface RemoteConsumer<T> {
    void accept(T t) throws TException;
  }
}
