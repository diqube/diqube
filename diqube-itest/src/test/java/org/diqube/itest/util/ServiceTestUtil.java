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
import org.diqube.connection.Connection;
import org.diqube.connection.ConnectionException;
import org.diqube.connection.ConnectionFactory;
import org.diqube.connection.SocketListener;
import org.diqube.itest.control.ServerControl;
import org.diqube.remote.cluster.thrift.ClusterFlattenService;
import org.diqube.remote.cluster.thrift.ClusterManagementService;
import org.diqube.remote.query.thrift.ClusterInformationService;
import org.diqube.remote.query.thrift.FlattenPreparationService;
import org.diqube.remote.query.thrift.IdentityService;
import org.diqube.remote.query.thrift.KeepAliveService;
import org.diqube.remote.query.thrift.QueryService;
import org.diqube.remote.query.thrift.TableMetadataService;
import org.diqube.thrift.base.services.DiqubeThriftServiceInfoManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utiltiy class to simply access the services of cluster servers in tests.
 *
 * @author Bastian Gloeckle
 */
public class ServiceTestUtil {
  private static final Logger logger = LoggerFactory.getLogger(ServiceTestUtil.class);
  private ServerControl server;
  private ConnectionFactory connectionFactory;
  private DiqubeThriftServiceInfoManager diqubeThriftServiceInfoManager;

  private static final SocketListener SOCKET_LISTENER = new SocketListener() {
    @Override
    public void connectionDied(String cause) {
      throw new RuntimeException("Connection died.");
    }
  };

  public ServiceTestUtil(ServerControl server, ConnectionFactory connectionFactory,
      DiqubeThriftServiceInfoManager diqubeThriftServiceInfoManager) {
    this.server = server;
    this.connectionFactory = connectionFactory;
    this.diqubeThriftServiceInfoManager = diqubeThriftServiceInfoManager;
  }

  /**
   * Open a connection to the {@link ClusterManagementService} of a specific node and then execute something.
   * 
   * @throws RuntimeException
   *           if anything goes wrong.
   */
  public void clusterMgmtService(RemoteConsumer<ClusterManagementService.Iface> execute) {
    try (Connection<ClusterManagementService.Iface> con = connectionFactory.createConnection(
        diqubeThriftServiceInfoManager.getServiceInfo(ClusterManagementService.Iface.class),
        server.getAddr().toRNodeAddress(), SOCKET_LISTENER)) {

      logger.info("Opened connection to ClusterManagementService at {}.", server.getAddr());
      execute.accept(con.getService());
      logger.info("Closing connection to ClusterManagementService at {}.", server.getAddr());

    } catch (TException | ConnectionException | IOException e) {
      logger.error("Exception while accessing ClusterManagementService of {}", server.getAddr(), e);
      throw new RuntimeException("Exception while accessing ClusterManagementService of " + server.getAddr(), e);
    }
  }

  /**
   * Open a connection to the {@link ClusterInformationService} of a specific node and then execute something.
   * 
   * @throws RuntimeException
   *           if anything goes wrong.
   */
  public void clusterInfoService(RemoteConsumer<ClusterInformationService.Iface> execute) {
    try (Connection<ClusterInformationService.Iface> con = connectionFactory.createConnection(
        diqubeThriftServiceInfoManager.getServiceInfo(ClusterInformationService.Iface.class),
        server.getAddr().toRNodeAddress(), SOCKET_LISTENER)) {

      logger.info("Opened connection to ClusterInformationService at {}.", server.getAddr());
      execute.accept(con.getService());
      logger.info("Closing connection to ClusterInformationService at {}.", server.getAddr());

    } catch (TException | ConnectionException | IOException e) {
      logger.error("Exception while accessing ClusterInformationService of {}", server.getAddr(), e);
      throw new RuntimeException("Exception while accessing ClusterInformationService of " + server.getAddr(), e);
    }
  }

  /**
   * Open a connection to the {@link QueryService} of a specific node and then execute something.
   * 
   * @throws RuntimeException
   *           if anything goes wrong.
   */
  public void queryService(RemoteConsumer<QueryService.Iface> execute) {
    try (Connection<QueryService.Iface> con =
        connectionFactory.createConnection(diqubeThriftServiceInfoManager.getServiceInfo(QueryService.Iface.class),
            server.getAddr().toRNodeAddress(), SOCKET_LISTENER)) {

      logger.info("Opened connection to QueryService at {}.", server.getAddr());
      execute.accept(con.getService());
      logger.info("Closing connection to QueryService at {}.", server.getAddr());

    } catch (TException | ConnectionException | IOException e) {
      logger.error("Exception while accessing QueryService of {}", server.getAddr(), e);
      throw new RuntimeException("Exception while accessing QueryService of " + server.getAddr(), e);
    }
  }

  /**
   * Open a connection to the {@link QueryService} of a specific node and then execute something.
   * 
   * @throws RuntimeException
   *           if anything on the connection goes wrong.
   */
  public void queryServiceThrowException(RemoteConsumer<QueryService.Iface> execute) throws TException {
    try (Connection<QueryService.Iface> con =
        connectionFactory.createConnection(diqubeThriftServiceInfoManager.getServiceInfo(QueryService.Iface.class),
            server.getAddr().toRNodeAddress(), SOCKET_LISTENER)) {

      logger.info("Opened connection to QueryService at {}.", server.getAddr());
      execute.accept(con.getService());
      logger.info("Closing connection to QueryService at {}.", server.getAddr());

    } catch (ConnectionException | IOException e) {
      logger.error("Exception while accessing QueryService of {}", server.getAddr(), e);
      throw new RuntimeException("Exception while accessing QueryService of " + server.getAddr(), e);
    }
  }

  /**
   * Open a connection to the {@link ClusterFlattenService} of a specific node and then execute something.
   * 
   * @throws RuntimeException
   *           if anything goes wrong.
   */
  public void clusterFlattenService(RemoteConsumer<ClusterFlattenService.Iface> execute) {
    try (Connection<ClusterFlattenService.Iface> con = connectionFactory.createConnection(
        diqubeThriftServiceInfoManager.getServiceInfo(ClusterFlattenService.Iface.class),
        server.getAddr().toRNodeAddress(), SOCKET_LISTENER)) {

      logger.info("Opened connection to ClusterFlattenService at {}.", server.getAddr());
      execute.accept(con.getService());
      logger.info("Closing connection to ClusterFlattenService at {}.", server.getAddr());

    } catch (TException | ConnectionException | IOException e) {
      logger.error("Exception while accessing ClusterFlattenService of {}", server.getAddr(), e);
      throw new RuntimeException("Exception while accessing ClusterFlattenService of " + server.getAddr(), e);
    }
  }

  /**
   * Open a connection to the {@link KeepAliveService} of a specific node and then execute something.
   * 
   * @throws RuntimeException
   *           if anything goes wrong.
   */
  public void keepAliveService(RemoteConsumer<KeepAliveService.Iface> execute) {
    try (Connection<KeepAliveService.Iface> con =
        connectionFactory.createConnection(diqubeThriftServiceInfoManager.getServiceInfo(KeepAliveService.Iface.class),
            server.getAddr().toRNodeAddress(), SOCKET_LISTENER)) {

      logger.info("Opened connection to KeepAliveService at {}.", server.getAddr());
      execute.accept(con.getService());
      logger.info("Closing connection to KeepAliveService at {}.", server.getAddr());

    } catch (TException | ConnectionException | IOException e) {
      // no logging here, as we use this to test if server is up -> we will have this case very often, but we do not
      // want to see that many stacktraces in the log...
      throw new RuntimeException("Exception while accessing KeepAliveService of " + server.getAddr(), e);
    }
  }

  /**
   * Open a connection to the {@link FlattenPreparationService} of a specific node and then execute something.
   * 
   * @throws RuntimeException
   *           if anything on the connection goes wrong.
   */
  public void flattenPreparationServiceThrowException(RemoteConsumer<FlattenPreparationService.Iface> execute)
      throws TException {
    try (Connection<FlattenPreparationService.Iface> con = connectionFactory.createConnection(
        diqubeThriftServiceInfoManager.getServiceInfo(FlattenPreparationService.Iface.class),
        server.getAddr().toRNodeAddress(), SOCKET_LISTENER)) {

      logger.info("Opened connection to FlattenPreparationService at {}.", server.getAddr());
      execute.accept(con.getService());
      logger.info("Closing connection to FlattenPreparationService at {}.", server.getAddr());

    } catch (ConnectionException | IOException e) {
      logger.error("Exception while accessing FlattenPreparationService of {}", server.getAddr(), e);
      throw new RuntimeException("Exception while accessing FlattenPreparationService of " + server.getAddr(), e);
    }
  }

  /**
   * Open a connection to the {@link IdentityService} of a specific node and then execute something.
   * 
   * @throws RuntimeException
   *           if anything goes wrong.
   */
  public void identityService(RemoteConsumer<IdentityService.Iface> execute) {
    try (Connection<IdentityService.Iface> con =
        connectionFactory.createConnection(diqubeThriftServiceInfoManager.getServiceInfo(IdentityService.Iface.class),
            server.getAddr().toRNodeAddress(), SOCKET_LISTENER)) {

      logger.info("Opened connection to IdentityService at {}.", server.getAddr());
      execute.accept(con.getService());
      logger.info("Closing connection to IdentityService at {}.", server.getAddr());

    } catch (TException | ConnectionException | IOException e) {
      logger.error("Exception while accessing IdentityService of {}", server.getAddr(), e);
      throw new RuntimeException("Exception while accessing IdentityService of " + server.getAddr(), e);
    }
  }

  /**
   * Open a connection to the {@link TableMetadataService} of a specific node and then execute something.
   * 
   * @throws RuntimeException
   *           if anything goes wrong.
   */
  public void tableMetadataService(RemoteConsumer<TableMetadataService.Iface> execute) {
    try (Connection<TableMetadataService.Iface> con = connectionFactory.createConnection(
        diqubeThriftServiceInfoManager.getServiceInfo(TableMetadataService.Iface.class),
        server.getAddr().toRNodeAddress(), SOCKET_LISTENER)) {

      logger.info("Opened connection to TableMetadataService at {}.", server.getAddr());
      execute.accept(con.getService());
      logger.info("Closing connection to TableMetadataService at {}.", server.getAddr());

    } catch (TException | ConnectionException | IOException e) {
      logger.error("Exception while accessing TableMetadataService of {}", server.getAddr(), e);
      throw new RuntimeException("Exception while accessing TableMetadataService of " + server.getAddr(), e);
    }
  }

  /**
   * Simple consumer of a thrift (remote) service, with correct thrift exceptioning.
   */
  public static interface RemoteConsumer<T> {
    void accept(T t) throws TException;
  }
}
