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
package org.diqube.tool.im;

import java.io.IOException;

import org.apache.thrift.TException;
import org.diqube.connection.Connection;
import org.diqube.connection.ConnectionException;
import org.diqube.connection.ConnectionPool;
import org.diqube.connection.NodeAddress;
import org.diqube.connection.SocketListener;
import org.diqube.context.Profiles;
import org.diqube.remote.query.thrift.IdentityService;
import org.diqube.remote.query.thrift.IdentityService.Iface;
import org.diqube.thrift.base.thrift.AuthenticationException;
import org.diqube.thrift.base.thrift.AuthorizationException;
import org.diqube.thrift.base.thrift.Ticket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * An "actual" function for identity service interaction.
 * 
 * <p>
 * Implementing classes need to have the {@link IsActualIdentityToolFunction} annotation!
 *
 * @author Bastian Gloeckle
 */
public abstract class AbstractActualIdentityToolFunction {
  private static final Logger logger = LoggerFactory.getLogger(AbstractActualIdentityToolFunction.class);

  private NodeAddress serverAddr;
  private String loginUser;
  private String loginPassword;
  private Connection<IdentityService.Iface> connection;
  private AnnotationConfigApplicationContext dataContext;

  public void initializeGeneral(String server, String loginUser, String loginPassword) {
    String[] serverSplit = server.split(":");
    if (serverSplit.length != 2)
      throw new RuntimeException("Server value needs to be ip:port.");
    String host = serverSplit[0];
    short port = Short.parseShort(serverSplit[1]);

    serverAddr = new NodeAddress(host, port);
    this.loginUser = loginUser;
    this.loginPassword = loginPassword;
  }

  public abstract void initializeOptionalParams(String paramUser, String paramPassword, String paramPermission,
      String paramPermissionObject, String paramEmail);

  /**
   * Subclasses implement the functionality on the given identityService in this method.
   */
  protected abstract void doExecute(Ticket ticket, Iface identityService)
      throws AuthenticationException, AuthorizationException, TException;

  /**
   * Execute this Identity tool function.
   */
  public void execute() {
    open();
    Ticket t = null;
    try {
      t = connection.getService().login(loginUser, loginPassword);
      doExecute(t, connection.getService());
    } catch (IllegalStateException | TException e) {
      throw new RuntimeException("Could not interact with identity service", e);
    } finally {
      if (t != null) {
        try {
          connection.getService().logout(t);
        } catch (IllegalStateException | TException e) {
          logger.warn("Could not logout.", e);
        }
      }
      close();
    }
  }

  private void open() {
    dataContext = new AnnotationConfigApplicationContext();
    dataContext.getEnvironment().setActiveProfiles(Profiles.CONFIG, Profiles.TOOL);
    dataContext.scan("org.diqube");
    dataContext.refresh();

    ConnectionPool connectionPool = dataContext.getBean(ConnectionPool.class);
    try {
      connection = connectionPool.reserveConnection(IdentityService.Iface.class, serverAddr.createRemote(),
          new SocketListener() {
            @Override
            public void connectionDied(String cause) {
              throw new RuntimeException("Connection died: " + cause);
            }
          });
    } catch (ConnectionException | InterruptedException e) {
      throw new RuntimeException("Could not open connection", e);
    }
  }

  public void close() {
    if (connection != null)
      try {
        connection.close();
      } catch (IOException e) {
        logger.warn("Could not close connection", e);
      }
    dataContext.close();

    connection = null;
    dataContext = null;
  }
}
